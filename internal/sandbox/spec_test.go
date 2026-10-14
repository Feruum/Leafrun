package sandbox

import (
	"strings"
	"testing"
)

func TestCreationOptionsEnforceSandbox(t *testing.T) {
	spec := Spec{JobID: "job", AttemptID: "attempt", CreateKey: "create", Image: "sha256:" + strings.Repeat("a", 64)}
	options, err := creationOptions(spec)
	if err != nil {
		t.Fatal(err)
	}
	c, h := options.Config, options.HostConfig
	if c.User != "65532:65532" || c.Tty || !c.OpenStdin || !c.NetworkDisabled || c.WorkingDir != "/work" || len(c.Entrypoint) != 1 || c.Entrypoint[0] != "/leafrun-supervisor" {
		t.Fatalf("unsafe process: %+v", c)
	}
	if !h.ReadonlyRootfs || h.NetworkMode != "none" || h.Privileged || len(h.Binds) != 0 || len(h.Mounts) != 0 || h.LogConfig.Type != "none" || h.AutoRemove {
		t.Fatalf("unsafe host policy: %+v", h)
	}
	if h.Memory != 512<<20 || h.MemorySwap != 512<<20 || h.NanoCPUs != 1e9 || h.PidsLimit == nil || *h.PidsLimit != 256 || h.Init == nil || *h.Init {
		t.Fatalf("unbounded resources or wrong PID1: %+v", h.Resources)
	}
	if len(h.Tmpfs) != 1 || !strings.Contains(h.Tmpfs["/work"], "size=268435456") || !strings.Contains(h.Tmpfs["/work"], "uid=65532") || len(h.CapDrop) != 1 || h.CapDrop[0] != "ALL" || len(h.SecurityOpt) != 1 || h.SecurityOpt[0] != "no-new-privileges" {
		t.Fatalf("unsafe filesystem/capabilities: %+v", h)
	}
	for _, env := range c.Env {
		if strings.Contains(env, "TOKEN") || strings.Contains(env, "AWS") || strings.Contains(env, "DOCKER") {
			t.Fatalf("credential in environment: %s", env)
		}
	}
	if options.Name != ContainerName(spec) || c.Labels["leafrun.create-key"] != spec.CreateKey {
		t.Fatal("create recovery identity differs")
	}
}

func TestCreationOptionsRejectMutableImageAndUnsafeIdentifiers(t *testing.T) {
	for _, spec := range []Spec{{JobID: "j", AttemptID: "a", CreateKey: "k", Image: "renderer:latest"}, {JobID: "j", AttemptID: "a", CreateKey: "bad\nkey", Image: "sha256:" + strings.Repeat("a", 64)}} {
		if _, err := creationOptions(spec); err == nil {
			t.Fatal("unsafe spec accepted")
		}
	}
}
