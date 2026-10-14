// A subprocess fixture for rejecting an unpinned engine before compilation.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Println("typst 0.15.0 (test engine)")
		return
	}
	if err := os.WriteFile(os.Args[len(os.Args)-1], []byte("%PDF-unpinned engine"), 0644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
