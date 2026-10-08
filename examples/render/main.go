package main

import (
	"fmt"
	"os"

	"github.com/Feruum/Leafrun"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: render <repository-path>")
		os.Exit(2)
	}
	pdf, err := leafrun.Render(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(pdf)
}
