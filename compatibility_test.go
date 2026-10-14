package leafrun

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRenderPreservesAbsoluteProjectAliasPath(t *testing.T) {
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "main.typ"), []byte("Project alias compatibility\n"), 0644); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "project alias")
	if runtime.GOOS == "windows" {
		if output, err := exec.Command("cmd", "/c", "mklink", "/J", alias, project).CombinedOutput(); err != nil {
			t.Fatalf("create project junction: %v: %s", err, output)
		}
	} else if err := os.Symlink(project, alias); err != nil {
		t.Fatal(err)
	}
	pdf, err := Render(alias)
	if err != nil {
		t.Fatal(err)
	}
	if pdf != filepath.Join(alias, "document.pdf") {
		t.Fatalf("legacy API changed project alias path: %q", pdf)
	}
	content, err := os.ReadFile(pdf)
	if err != nil || !strings.HasPrefix(string(content), "%PDF-") {
		t.Fatalf("expected a PDF through project alias: %v", err)
	}
}

func TestVersionMismatchPreservesExistingResult(t *testing.T) {
	tools := t.TempDir()
	binary := filepath.Join(tools, "typst")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, "./testdata/typst")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build test engine: %v\n%s", err, output)
	}
	t.Setenv("PATH", tools+string(os.PathListSeparator)+os.Getenv("PATH"))
	project := t.TempDir()
	if err := os.WriteFile(filepath.Join(project, "main.typ"), []byte("= Hello"), 0644); err != nil {
		t.Fatal(err)
	}
	previous := filepath.Join(project, "document.pdf")
	if err := os.WriteFile(previous, []byte("previous PDF sentinel"), 0644); err != nil {
		t.Fatal(err)
	}
	pdf, err := Render(project)
	if pdf != "" || err == nil || !strings.Contains(err.Error(), "0.15.1") {
		t.Fatalf("expected pinned engine failure, got PDF %q and error %v", pdf, err)
	}
	content, err := os.ReadFile(previous)
	if err != nil || string(content) != "previous PDF sentinel" {
		t.Fatalf("old PDF changed: %q, %v", content, err)
	}
}
