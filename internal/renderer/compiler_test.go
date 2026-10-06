package renderer

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func typstConfig(t *testing.T) Config {
	t.Helper()
	cfg := testConfig(t)
	if path := os.Getenv("TYPST_BIN"); path != "" {
		cfg.TypstBin = path
	} else {
		t.Skip("set TYPST_BIN to run real Typst PDF integration tests")
	}
	return cfg
}

func TestCompilerRealPDFAndInputIsolation(t *testing.T) {
	cfg := typstConfig(t)
	f := fixture(t)
	f.commit(t, "chapters/body.typ", "#let data = json(sys.inputs.data_file)\n= #data.title\n#image(\"/assets/logo.svg\", width: 1cm)\n#set text(font: \"Libertinus Serif\")\nПривет, мир!")
	f.commit(t, "assets/logo.svg", "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"20\" height=\"20\"><rect width=\"20\" height=\"20\" fill=\"green\"/></svg>")
	f.commit(t, "main.typ", "#include \"chapters/body.typ\"")
	gitRun(t, f.source, "push", "origin", "main")
	c := makeCache(t, cfg)
	co := prepare(t, c, f.repo(), "HEAD")
	pdf, err := Compile(context.Background(), cfg, co, "main.typ", json.RawMessage("{\"title\":\"Проверка JSON\"}"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(pdf)
	if err != nil || !strings.HasPrefix(string(content), "%PDF-") {
		t.Fatalf("not a PDF: %v", err)
	}
	if containsPath(co.ProjectDir, pdf) {
		t.Fatal("compiler output was written into the source project")
	}
	entries, _ := os.ReadDir(cfg.CacheDir)
	if len(entries) != 1 {
		t.Fatal("compiler modified repository cache structure")
	}
}

func TestCompilerErrorsAndPathContainment(t *testing.T) {
	cfg := typstConfig(t)
	f := fixture(t)
	c := makeCache(t, cfg)
	co := prepare(t, c, f.repo(), "HEAD")
	for _, entry := range []string{"../secret.typ", "/etc/passwd", "-bad.typ", "C:\\secret.typ", "main.typ\x00"} {
		_, err := Compile(context.Background(), cfg, co, entry, json.RawMessage("{}"))
		requireErrorCode(t, err, "invalid_entrypoint", http.StatusBadRequest)
	}
	_, err := Compile(context.Background(), cfg, co, "missing.typ", json.RawMessage("{}"))
	requireErrorCode(t, err, "invalid_entrypoint", http.StatusBadRequest)
	os.WriteFile(filepath.Join(co.ProjectDir, "main.typ"), []byte("#let broken = ("), 0600)
	_, err = Compile(context.Background(), cfg, co, "main.typ", json.RawMessage("{}"))
	requireErrorCode(t, err, "typst_error", http.StatusUnprocessableEntity)
	if asAPIError(err).Diagnostics == "" {
		t.Fatal("Typst diagnostics were discarded")
	}
}

func TestCompilerDeadlineAndCleanup(t *testing.T) {
	cfg := typstConfig(t)
	f := fixture(t)
	f.commit(t, "main.typ", "#let values = range(1000000000).map(x => x * x)\n#values.len()")
	gitRun(t, f.source, "push", "origin", "main")
	c := makeCache(t, cfg)
	co := prepare(t, c, f.repo(), "HEAD")
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Compile(ctx, cfg, co, "main.typ", json.RawMessage("{}"))
	requireErrorCode(t, err, "timeout", http.StatusGatewayTimeout)
	if time.Since(start) > 10*time.Second {
		t.Fatal("compiler did not stop on deadline")
	}
	if err := co.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(co.RequestDir); !os.IsNotExist(err) {
		t.Fatal("timed-out compiler retained checkout")
	}
}

func TestCompilerNestedEntrypointAndProjectFont(t *testing.T) {
	cfg := typstConfig(t)
	var font []byte
	family, pdfName := "DejaVu Sans", "DejaVuSans"
	for _, candidate := range []string{"/usr/share/fonts/dejavu/DejaVuSans.ttf", "/usr/share/fonts/ttf-dejavu/DejaVuSans.ttf", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", `C:\Windows\Fonts\arial.ttf`} {
		if content, err := os.ReadFile(candidate); err == nil {
			font = content
			if strings.Contains(candidate, "arial") {
				family, pdfName = "Arial", "Arial"
			}
			break
		}
	}
	if len(font) == 0 {
		t.Skip("install a test font (DejaVu Sans or Arial)")
	}
	f := fixture(t)
	f.commit(t, "fonts/project.ttf", string(font))
	f.commit(t, "reports/main.typ", "#let data = json(sys.inputs.data_file)\n#set text(font: \""+family+"\")\n= #data.title")
	gitRun(t, f.source, "push", "origin", "main")
	co := prepare(t, makeCache(t, cfg), f.repo(), "HEAD")
	pdf, err := Compile(context.Background(), cfg, co, "reports/main.typ", json.RawMessage(`{"title":"Custom font"}`))
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(pdf)
	if err != nil || !strings.Contains(string(content), pdfName) {
		t.Fatalf("PDF did not embed the project font: %v", err)
	}
}

func TestCompilerOfficialPreviewPackage(t *testing.T) {
	cfg := typstConfig(t)
	f := fixture(t)
	f.commit(t, "main.typ", "#import \"@preview/tablex:0.0.9\": *\n= Official package test")
	gitRun(t, f.source, "push", "origin", "main")
	co := prepare(t, makeCache(t, cfg), f.repo(), "HEAD")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := Compile(ctx, cfg, co, "main.typ", json.RawMessage("{}")); err != nil {
		t.Fatalf("%v: %s", err, asAPIError(err).Diagnostics)
	}
	if _, err := os.Stat(filepath.Join(co.RequestDir, "packages", "preview", "tablex", "0.0.9", "typst.toml")); err != nil {
		t.Fatal("official package was not downloaded into the request")
	}
}
