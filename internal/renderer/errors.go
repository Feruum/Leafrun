package renderer

import (
	"context"
	"errors"
	"net/http"
)

type APIError struct {
	Status      int    `json:"-"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	Diagnostics string `json:"diagnostics,omitempty"`
}

func (e *APIError) Error() string { return e.Message }

func failure(status int, code, message string) *APIError {
	return &APIError{Status: status, Code: code, Message: message}
}

func asAPIError(err error) *APIError {
	if errors.Is(err, context.DeadlineExceeded) {
		return failure(http.StatusGatewayTimeout, "timeout", "The render request timed out.")
	}
	if errors.Is(err, context.Canceled) {
		return failure(http.StatusRequestTimeout, "canceled", "The render request was canceled.")
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return failure(http.StatusInternalServerError, "internal_error", "The render could not be completed.")
}
