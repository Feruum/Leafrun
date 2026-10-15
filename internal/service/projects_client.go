package service

import (
	"context"

	projectsv1 "github.com/leafrun/leafrun-contracts/projects/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

// ProjectsClient is the fixed-credential owner surface used to resolve fresh
// render selections and to recheck a release immediately before admission and
// publication.
type ProjectsClient interface {
	Resolve(context.Context, projectsv1.ResolveRequest) (projectsv1.ResolvedExecution, error)
	CurrentRelease(context.Context, projectsv1.CurrentReleaseCheck) (projectsv1.CurrentReleaseDecision, error)
}

// ResourceMetadataReader is the Storage metadata surface used before a
// resolved file reference is admitted or its bytes are read.
type ResourceMetadataReader interface {
	GetResource(context.Context, string) (storagev1.Resource, error)
}
