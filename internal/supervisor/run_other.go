//go:build !linux

package supervisor

import (
	"context"
	"errors"
)

func Run(context.Context, Options) error { return errors.New("renderer supervisor requires Linux") }
