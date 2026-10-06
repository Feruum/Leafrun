package renderer

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func CheckRuntime(cfg Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := runCommand(ctx, cfg.GitBin, []string{"--version"}, "", nil); err != nil {
		return fmt.Errorf("Git CLI is unavailable")
	}
	version, err := runCommand(ctx, cfg.TypstBin, []string{"--version"}, "", nil)
	if err != nil {
		return fmt.Errorf("Typst CLI is unavailable")
	}
	if !strings.HasPrefix(version, "typst 0.15.1 ") && version != "typst 0.15.1" {
		return fmt.Errorf("Typst CLI 0.15.1 is required")
	}
	return nil
}
