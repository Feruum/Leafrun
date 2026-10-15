package renderer

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	"github.com/leafrun/leafrun-contracts/renderwire/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	documentcore "github.com/leafrun/leafrun-document-core"
)

func stageInputFiles(ctx context.Context, decoder *renderwire.Decoder, workRoot string, files []projectsv1.PreparedFile) (map[string]string, error) {
	plan, err := renderwire.PlanInputFiles(files)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]string, len(plan))
	if len(plan) == 0 {
		return paths, nil
	}
	root := filepath.Join(workRoot, "input-files")
	if err = os.Mkdir(root, 0o700); err != nil {
		return nil, err
	}
	for index, file := range plan {
		paths[file.Path] = filepath.Join(root, fmt.Sprintf("file-%02d", index))
	}
	err = renderwire.ReadInputFiles(decoder, files, func(file projectsv1.PreparedFile, reader io.Reader) error {
		path, ok := paths[file.Path]
		if !ok {
			return errors.New("input frame has no private staging path")
		}
		output, createErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if createErr != nil {
			return createErr
		}
		_, copyErr := copyContext(ctx, output, reader)
		syncErr := output.Sync()
		closeErr := output.Close()
		return errors.Join(copyErr, syncErr, closeErr)
	})
	if err != nil {
		return nil, err
	}
	return paths, nil
}

func prepareSelection(ctx context.Context, base *documentcore.Snapshot, request renderwire.RenderRequest, staged map[string]string, workRoot string) (projectsv1.TargetDescriptor, *documentcore.Snapshot, error) {
	if base == nil || request.Execution == nil {
		return projectsv1.TargetDescriptor{}, nil, errors.New("prepared execution is required")
	}
	manifest, err := documentcore.ReadProject(ctx, base.FS())
	if err != nil {
		return projectsv1.TargetDescriptor{}, nil, err
	}
	coreDescriptor, err := documentcore.DescribeTarget(ctx, base.FS(), manifest, base.Manifest(), request.TargetID, request.Execution.Profile)
	if err != nil {
		return projectsv1.TargetDescriptor{}, nil, err
	}
	if coreDescriptor.Kind != documentcore.ProjectTargetDocument || coreDescriptor.TargetID != request.TargetID || coreDescriptor.Entrypoint != request.Entrypoint {
		return projectsv1.TargetDescriptor{}, nil, errors.New("captured target differs from frozen document execution")
	}
	descriptorBytes, err := json.Marshal(coreDescriptor)
	if err != nil {
		return projectsv1.TargetDescriptor{}, nil, err
	}
	var descriptor projectsv1.TargetDescriptor
	if err = jsonvalue.Unmarshal(descriptorBytes, &descriptor, 4<<20); err != nil {
		return projectsv1.TargetDescriptor{}, nil, err
	}
	if descriptor.Digest != request.Execution.DescriptorDigest || descriptor.ExecutionDigest != request.Execution.ExecutionDigest || descriptor.SelectedProfile != request.Execution.Profile || request.ParametersSchemaPath != descriptor.InputSchema {
		return projectsv1.TargetDescriptor{}, nil, errors.New("captured target descriptor differs from frozen execution digests")
	}
	preparedBytes, err := json.Marshal(request.Execution.Prepared)
	if err != nil {
		return projectsv1.TargetDescriptor{}, nil, err
	}
	var prepared documentcore.PreparedInput
	if err = jsonvalue.Unmarshal(preparedBytes, &prepared, projectsv1.ResponseLimit); err != nil {
		return projectsv1.TargetDescriptor{}, nil, err
	}
	if len(request.Parameters) == 0 || !bytes.Equal(request.Parameters, prepared.JSON) {
		return projectsv1.TargetDescriptor{}, nil, errors.New("render parameters differ from the frozen prepared JSON")
	}
	normalized, err := jsonvalue.NormalizeObject(prepared.JSON)
	if err != nil || !bytes.Equal(normalized, prepared.JSON) {
		return projectsv1.TargetDescriptor{}, nil, errors.New("frozen prepared JSON is not canonical")
	}
	expectedInputs := cloneStringMap(coreDescriptor.ResolvedProfile.Inputs)
	if _, reserved := expectedInputs["leafrun-data"]; reserved {
		return projectsv1.TargetDescriptor{}, nil, errors.New("project profile attempts to override the reserved input")
	}
	expectedInputs["leafrun-data"] = "/input.json"
	if !reflect.DeepEqual(expectedInputs, prepared.Inputs) {
		return projectsv1.TargetDescriptor{}, nil, errors.New("prepared input map differs from the selected profile")
	}
	if err = documentcore.VerifyPreparedInput(ctx, base.FS(), descriptor.InputSchema, prepared); err != nil {
		return projectsv1.TargetDescriptor{}, nil, err
	}
	if len(prepared.Files) != len(request.Execution.Prepared.Files) {
		return projectsv1.TargetDescriptor{}, nil, errors.New("prepared file descriptor conversion changed its length")
	}
	for _, file := range prepared.Files {
		if staged[file.Path] == "" {
			return projectsv1.TargetDescriptor{}, nil, errors.New("prepared input file was not staged")
		}
	}
	derived, err := documentcore.CreatePreparedSnapshot(ctx, base, prepared, func(_ context.Context, file documentcore.PreparedFile) (io.ReadCloser, error) {
		path := staged[file.Path]
		if path == "" || !strings.HasPrefix(filepath.Clean(path), filepath.Clean(filepath.Join(workRoot, "input-files"))+string(os.PathSeparator)) {
			return nil, errors.New("prepared file path is outside private staging")
		}
		return os.Open(path)
	}, documentcore.CreateInputSnapshotOptions{TempDir: workRoot, Limits: snapshotLimits})
	if err != nil {
		return projectsv1.TargetDescriptor{}, nil, err
	}
	return descriptor, derived, nil
}

func validateSelectionRequest(request renderwire.RenderRequest, manifest storagev1.SnapshotManifest) error {
	if request.Execution == nil {
		return errors.New("prepared execution is required")
	}
	if !validIdentifier(request.Scope.WorkspaceID) || !validIdentifier(request.Scope.ProjectID) || !validIdentifier(request.JobID) || !validIdentifier(request.AttemptID) || !validIdentifier(request.SourceSnapshotID) || !validIdentifier(request.TargetID) || !generationv1.LogicalFilePath(request.Entrypoint) || request.SourceContentID != manifest.ID {
		return errors.New("render selection identity or source binding is invalid")
	}
	if !lowerDigest(request.InputFingerprint) || !lowerDigest(request.Execution.DescriptorDigest) || !lowerDigest(request.Execution.ExecutionDigest) || !validIdentifier(request.Execution.Profile) || request.ParametersSchemaPath == "" || !generationv1.LogicalFilePath(request.ParametersSchemaPath) {
		return errors.New("render execution digest or profile binding is invalid")
	}
	if len(request.Parameters) == 0 || !bytes.Equal(request.Parameters, request.Execution.Prepared.JSON) {
		return errors.New("render parameters differ from the frozen prepared input")
	}
	normalized, err := jsonvalue.NormalizeObject(request.Parameters)
	if err != nil || !bytes.Equal(normalized, request.Parameters) {
		return errors.New("render parameters are not exact normalized JSON")
	}
	if _, err = renderwire.PlanInputFiles(request.Execution.Prepared.Files); err != nil {
		return err
	}
	return nil
}

func lowerDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}

func cloneStringMap(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source)+1)
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
