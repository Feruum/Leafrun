// Package supervisor supplies the renderer's independent PID 1 timeout.
package supervisor

import (
	"errors"
	"io"
	"time"
)

var ErrTimeout = errors.New("renderer exceeded its independent attempt timeout")

// ErrReapTimeout reports that the forced-kill wait exceeded its bounded grace.
var ErrReapTimeout = errors.New("renderer process did not exit after forced kill")

type Options struct {
	Command        string
	Args           []string
	Env            []string
	Stdin          io.Reader
	Stdout, Stderr io.Writer
	Timeout        time.Duration
}
