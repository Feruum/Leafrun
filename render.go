package leafrun

import (
	"context"
	"path/filepath"

	documentcore "github.com/leafrun/leafrun-document-core"
)

// Render compiles main.typ in a local project and returns its document.pdf path.
// Typst CLI 0.15.1 must be available in PATH. A successful render atomically
// replaces document.pdf; failures preserve the previous output.
func Render(repoPath string) (string, error) {
	root, err := filepath.Abs(repoPath)
	if err != nil {
		return "", err
	}
	_, err = documentcore.Render(context.Background(), documentcore.RenderOptions{
		ProjectDir: root,
		Entrypoint: "main.typ",
		Output:     "document.pdf",
	})
	if err != nil {
		return "", err
	}
	// Preserve the legacy path spelling even when the core resolves a project
	// alias to its canonical directory.
	return filepath.Join(root, "document.pdf"), nil
}
