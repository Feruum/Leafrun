// Package sandbox controls one fixed-policy Linux renderer container. Only the
// controller has Docker access; the renderer receives bytes through attached IO.
package sandbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

var imageID = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var exactID = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Spec struct {
	JobID     string `json:"jobId"`
	AttemptID string `json:"attemptId"`
	CreateKey string `json:"createKey"`
	Image     string `json:"image"`
}

func ContainerName(s Spec) string {
	hash := sha256.Sum256([]byte(s.JobID + "\x00" + s.AttemptID + "\x00" + s.CreateKey))
	return "leafrun-render-" + hex.EncodeToString(hash[:])
}
func creationOptions(s Spec) (client.ContainerCreateOptions, error) {
	if !imageID.MatchString(s.Image) {
		return client.ContainerCreateOptions{}, errors.New("renderer must use an immutable local image ID")
	}
	for _, value := range []string{s.JobID, s.AttemptID, s.CreateKey} {
		if value == "" || len(value) > 256 || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return client.ContainerCreateOptions{}, errors.New("invalid container recovery identity")
		}
	}
	pids := int64(256)
	init := false
	stop := 1
	return client.ContainerCreateOptions{Name: ContainerName(s), Config: &container.Config{Image: s.Image, User: "65532:65532", WorkingDir: "/work", Entrypoint: []string{"/leafrun-supervisor"}, Cmd: []string{"--timeout=60s"}, OpenStdin: true, StdinOnce: true, AttachStdin: true, AttachStdout: true, AttachStderr: true, NetworkDisabled: true, StopTimeout: &stop, StopSignal: "SIGTERM", Env: []string{"PATH=/usr/local/bin:/usr/bin:/bin", "SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt", "HOME=/work", "TMPDIR=/work"}, Labels: map[string]string{"leafrun.job": s.JobID, "leafrun.attempt": s.AttemptID, "leafrun.create-key": s.CreateKey}}, HostConfig: &container.HostConfig{ReadonlyRootfs: true, NetworkMode: "none", LogConfig: container.LogConfig{Type: "none"}, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges"}, Init: &init, Tmpfs: map[string]string{"/work": "rw,noexec,nosuid,nodev,size=268435456,mode=0700,uid=65532,gid=65532"}, Resources: container.Resources{Memory: 512 << 20, MemorySwap: 512 << 20, NanoCPUs: 1e9, PidsLimit: &pids}}}, nil
}

type State struct {
	ID                                   string
	Running, Dead, Restarting, OOMKilled bool
	ExitCode                             int
	Status                               string
}

func (s State) Stopped() bool {
	return !s.Running && !s.Restarting && !s.Dead && (s.Status == "exited" || s.Status == "created")
}
func (s State) Successful() bool {
	return s.Stopped() && s.Status == "exited" && !s.OOMKilled && s.ExitCode == 0
}

type Docker struct{ client *client.Client }

func NewDocker() (*Docker, error) {
	c, err := client.New(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &Docker{client: c}, nil
}
func (d *Docker) Close() error { return d.client.Close() }
func (d *Docker) Ready(ctx context.Context) error {
	info, err := d.client.Info(ctx, client.InfoOptions{})
	if err != nil {
		return err
	}
	if info.Info.OSType != "linux" || !info.Info.MemoryLimit || !info.Info.SwapLimit || !info.Info.CPUCfsQuota {
		return errors.New("Docker provider lacks required Linux resource limits")
	}
	return nil
}
func (d *Docker) Create(ctx context.Context, s Spec) (string, error) {
	options, err := creationOptions(s)
	if err != nil {
		return "", err
	}
	response, err := d.client.ContainerCreate(ctx, options)
	if err != nil {
		return "", err
	}
	if !exactID.MatchString(response.ID) {
		return "", errors.New("Docker returned an invalid container identity")
	}
	return response.ID, nil
}

// Inspect verifies the entire immutable isolation policy before an ID can be
// adopted from its deterministic name after a lost Docker create response.
func (d *Docker) Inspect(ctx context.Context, id string, s Spec) (State, error) {
	result, err := d.client.ContainerInspect(ctx, id, client.ContainerInspectOptions{})
	if err != nil {
		return State{}, err
	}
	c := result.Container
	expected, err := creationOptions(s)
	if err != nil {
		return State{}, err
	}
	if c.Config == nil || c.HostConfig == nil || c.State == nil || !exactID.MatchString(c.ID) {
		return State{}, errors.New("Docker inspection is incomplete")
	}
	config, host := c.Config, c.HostConfig
	if c.Image != s.Image || strings.TrimPrefix(c.Name, "/") != expected.Name || config.Labels["leafrun.job"] != s.JobID || config.Labels["leafrun.attempt"] != s.AttemptID || config.Labels["leafrun.create-key"] != s.CreateKey || config.User != expected.Config.User || !config.NetworkDisabled || config.Tty || config.WorkingDir != "/work" || !slices.Equal(config.Entrypoint, expected.Config.Entrypoint) || !slices.Equal(config.Cmd, expected.Config.Cmd) || !slices.Equal(config.Env, expected.Config.Env) || !host.ReadonlyRootfs || host.NetworkMode != "none" || host.Privileged || len(host.Binds) > 0 || len(host.Mounts) > 0 || len(host.Devices) > 0 || len(host.DeviceRequests) > 0 || host.LogConfig.Type != "none" || host.AutoRemove || host.Init == nil || *host.Init || host.Memory != expected.HostConfig.Memory || host.MemorySwap != expected.HostConfig.MemorySwap || host.NanoCPUs != expected.HostConfig.NanoCPUs || host.PidsLimit == nil || *host.PidsLimit != 256 || len(host.Tmpfs) != 1 || host.Tmpfs["/work"] != expected.HostConfig.Tmpfs["/work"] || !slices.Equal(host.CapDrop, expected.HostConfig.CapDrop) || !slices.Equal(host.SecurityOpt, expected.HostConfig.SecurityOpt) {
		return State{}, fmt.Errorf("container does not match its durable sandbox identity/policy")
	}
	return State{ID: c.ID, Running: c.State.Running, Dead: c.State.Dead, Restarting: c.State.Restarting, OOMKilled: c.State.OOMKilled, ExitCode: c.State.ExitCode, Status: string(c.State.Status)}, nil
}
func (d *Docker) Attach(ctx context.Context, id string) (client.ContainerAttachResult, error) {
	if !exactID.MatchString(id) {
		return client.ContainerAttachResult{}, errors.New("exact container ID required")
	}
	return d.client.ContainerAttach(ctx, id, client.ContainerAttachOptions{Stream: true, Stdin: true, Stdout: true, Stderr: true, Logs: false})
}
func (d *Docker) Start(ctx context.Context, id string) error {
	if !exactID.MatchString(id) {
		return errors.New("exact container ID required")
	}
	_, err := d.client.ContainerStart(ctx, id, client.ContainerStartOptions{})
	return err
}
func (d *Docker) Kill(ctx context.Context, id string) error {
	if !exactID.MatchString(id) {
		return errors.New("exact container ID required")
	}
	_, err := d.client.ContainerKill(ctx, id, client.ContainerKillOptions{Signal: "SIGKILL"})
	return err
}
func (d *Docker) Remove(ctx context.Context, id string) error {
	if !exactID.MatchString(id) {
		return errors.New("exact container ID required")
	}
	_, err := d.client.ContainerRemove(ctx, id, client.ContainerRemoveOptions{RemoveVolumes: false, Force: false})
	return err
}
