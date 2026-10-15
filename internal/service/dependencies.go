package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const dependencyResponseLimit = 1 << 20

// HTTPStatusError carries a status and an allowlisted recovery code. Response
// text is excluded because dependency bodies may contain private data.
type HTTPStatusError struct {
	StatusCode int
	Code       string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("dependency HTTP status %d", e.StatusCode)
}

type dependencyHTTP struct {
	baseURL string
	token   string
	client  *http.Client
}

func newDependencyHTTP(baseURL, token string, transport *http.Client) (*dependencyHTTP, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || token == "" {
		return nil, errors.New("dependency URL and fixed service token are required")
	}
	if transport == nil {
		transport = &http.Client{Timeout: 10 * time.Second}
	}
	copy := *transport
	if copy.Timeout <= 0 {
		copy.Timeout = 10 * time.Second
	}
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &dependencyHTTP{baseURL: strings.TrimRight(baseURL, "/"), token: token, client: &copy}, nil
}

func (d *dependencyHTTP) call(ctx context.Context, method, path string, input any, output any) error {
	return d.callBounded(ctx, method, path, input, output, dependencyResponseLimit)
}

func (d *dependencyHTTP) callBounded(ctx context.Context, method, path string, input any, output any, responseLimit int64) error {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return err
		}
		if len(body) > jsonvalue.MaxBytes {
			return errors.New("dependency request exceeds 1 MiB")
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, d.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+d.token)
	request.Header.Set("Accept", "application/json")
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := d.client.Do(request)
	if err != nil {
		return errors.New("dependency request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		failure := &HTTPStatusError{StatusCode: response.StatusCode}
		if response.StatusCode == http.StatusConflict {
			data, readErr := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
			var envelope storagev1.ErrorResponse
			if readErr == nil && len(data) <= 64<<10 && jsonvalue.Unmarshal(data, &envelope, 64<<10) == nil && envelope.Error.Code == "pin_expired" {
				failure.Code = "pin_expired"
			}
		}
		return failure
	}
	if output == nil {
		return nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		return &HTTPStatusError{StatusCode: response.StatusCode}
	}
	contentType := response.Header.Get("Content-Type")
	if !strings.HasPrefix(strings.ToLower(contentType), "application/json") {
		return errors.New("dependency returned a non-JSON response")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, responseLimit+1))
	if err != nil {
		return errors.New("dependency response could not be read")
	}
	if int64(len(data)) > responseLimit {
		return errors.New("dependency response exceeds limit")
	}
	if err = jsonvalue.Unmarshal(data, output, responseLimit); err != nil {
		return errors.New("dependency response is invalid")
	}
	return nil
}

// AccessClient makes strict, bounded owner/access checks. It carries one fixed
// Generation service credential and never follows redirects.
type AccessClient struct{ http *dependencyHTTP }

func NewAccessClient(baseURL, fixedToken string, transport *http.Client) (*AccessClient, error) {
	dependency, err := newDependencyHTTP(baseURL, fixedToken, transport)
	if err != nil {
		return nil, err
	}
	return &AccessClient{http: dependency}, nil
}

func (c *AccessClient) Check(ctx context.Context, check storagev1.AccessCheck) (bool, error) {
	var decision storagev1.AccessDecision
	if err := c.http.call(ctx, http.MethodPost, "/internal/v1/checks/access", check, &decision); err != nil {
		return false, err
	}
	return decision.Allowed, nil
}

func (c *AccessClient) Ready(ctx context.Context) error {
	_, err := c.Check(ctx, storagev1.AccessCheck{Principal: storagev1.Principal{Subject: "leafrun-generation", Domain: "service"}, Scope: storagev1.Scope{WorkspaceID: "__readiness__", ProjectID: "__readiness__"}, Operation: "health"})
	return err
}

func (c *PinClient) Ready(ctx context.Context) error {
	return c.http.call(ctx, http.MethodGet, "/readyz", nil, nil)
}

// PinClient is the bounded Storage adapter used by admission. Its token is a
// fixed Generation service credential, never the caller's bearer token.
type PinClient struct{ http *dependencyHTTP }

func NewPinClient(baseURL, fixedToken string, transport *http.Client) (*PinClient, error) {
	dependency, err := newDependencyHTTP(baseURL, fixedToken, transport)
	if err != nil {
		return nil, err
	}
	return &PinClient{http: dependency}, nil
}

func (c *PinClient) AcquirePin(ctx context.Context, in storagev1.AcquirePinRequest) (storagev1.Pin, error) {
	var pin storagev1.Pin
	err := c.http.call(ctx, http.MethodPost, "/internal/v1/pins", in, &pin)
	return pin, err
}

func (c *PinClient) GetSnapshot(ctx context.Context, snapshotID string) (storagev1.Snapshot, error) {
	if !validIdentifier(snapshotID) || strings.ContainsAny(snapshotID, "/\\") {
		return storagev1.Snapshot{}, errors.New("invalid snapshot ID")
	}
	var snapshot storagev1.Snapshot
	err := c.http.callBounded(ctx, http.MethodGet, "/internal/v1/snapshots/"+url.PathEscape(snapshotID), nil, &snapshot, 16<<20)
	return snapshot, err
}

func (c *PinClient) GetResource(ctx context.Context, resourceID string) (storagev1.Resource, error) {
	if !validIdentifier(resourceID) || strings.ContainsAny(resourceID, "/\\") {
		return storagev1.Resource{}, errors.New("invalid resource ID")
	}
	var resource storagev1.Resource
	err := c.http.callBounded(ctx, http.MethodGet, "/internal/v1/resources/"+url.PathEscape(resourceID), nil, &resource, 1<<20)
	return resource, err
}

func (c *PinClient) GetPin(ctx context.Context, pinID string) (storagev1.Pin, error) {
	if !validIdentifier(pinID) || strings.ContainsAny(pinID, "/\\") {
		return storagev1.Pin{}, errors.New("invalid pin ID")
	}
	var pin storagev1.Pin
	err := c.http.call(ctx, http.MethodGet, "/internal/v1/pins/"+url.PathEscape(pinID), nil, &pin)
	return pin, err
}

func (c *PinClient) ActivatePin(ctx context.Context, pinID string) (storagev1.Pin, error) {
	if !validIdentifier(pinID) || strings.ContainsAny(pinID, "/\\") {
		return storagev1.Pin{}, errors.New("invalid pin ID")
	}
	var pin storagev1.Pin
	err := c.http.call(ctx, http.MethodPost, "/internal/v1/pins/"+url.PathEscape(pinID)+"/activate", nil, &pin)
	return pin, err
}
