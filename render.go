package leafrun

import (
	"fmt"
	"os/exec"
	"path/filepath"
)

// Render compiles main.typ in a local project and returns its document.pdf path.
// Typst CLI must be available in PATH. An existing document.pdf is overwritten.
func Render(repoPath string) (string, error) {
	root, err := filepath.Abs(repoPath)
	if err != nil {
		return "", err
	}
	pdf := filepath.Join(root, "document.pdf")
	cmd := exec.Command("typst", "compile", "--root", root, filepath.Join(root, "main.typ"), pdf)
	cmd.Dir = root
	if output, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("typst render: %w\n%s", err, output)
	}
	return pdf, nil
}
