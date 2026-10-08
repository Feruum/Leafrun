package leafrun

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRender(t *testing.T) {
	if _, err := exec.LookPath("typst"); err != nil {
		t.Skip("install Typst CLI and add it to PATH")
	}

	t.Run("local files and relative project path", func(t *testing.T) {
		project := filepath.Join(t.TempDir(), "project with spaces")
		files := map[string]string{
			"main.typ":          "#include \"sections/body.typ\"",
			"sections/body.typ": "#let data = json(\"/data.json\")\n= #data.title\n#image(\"/assets/logo.svg\", width: 1cm)",
			"data.json":         "{\"title\":\"Проверка PDF\"}",
			"assets/logo.svg":   "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"20\" height=\"20\"><rect width=\"20\" height=\"20\" fill=\"green\"/></svg>",
		}
		for name, content := range files {
			path := filepath.Join(project, filepath.FromSlash(name))
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
		}
		t.Chdir(t.TempDir())
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		relative, err := filepath.Rel(cwd, project)
		if err != nil {
			t.Fatal(err)
		}
		pdf, err := Render(relative)
		if err != nil {
			t.Fatal(err)
		}
		if pdf != filepath.Join(project, "document.pdf") {
			t.Fatalf("unexpected PDF path: %s", pdf)
		}
		content, err := os.ReadFile(pdf)
		if err != nil || !strings.HasPrefix(string(content), "%PDF-") {
			t.Fatalf("expected a PDF, got read error %v", err)
		}
	})

	t.Run("Typst error includes diagnostics", func(t *testing.T) {
		project := t.TempDir()
		if err := os.WriteFile(filepath.Join(project, "main.typ"), []byte("#let broken = ("), 0644); err != nil {
			t.Fatal(err)
		}
		pdf, err := Render(project)
		var exitError *exec.ExitError
		if pdf != "" || !errors.As(err, &exitError) || !strings.Contains(err.Error(), "error:") {
			t.Fatalf("expected Typst diagnostics, got %q, %v", pdf, err)
		}
	})

	t.Run("missing main.typ returns an error", func(t *testing.T) {
		pdf, err := Render(t.TempDir())
		if err == nil || pdf != "" || !strings.Contains(err.Error(), "main.typ") {
			t.Fatalf("expected missing main.typ error, got %q, %v", pdf, err)
		}
	})
}
