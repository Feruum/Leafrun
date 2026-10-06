package renderer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type RenderRequest struct {
	RepoURL    string          `json:"repo_url"`
	Ref        string          `json:"ref,omitempty"`
	Entrypoint string          `json:"entrypoint,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

type Server struct {
	cfg   Config
	cache *Cache
	log   *slog.Logger
	slots chan struct{}
}

func NewServer(cfg Config, cache *Cache, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{cfg: cfg, cache: cache, log: logger, slots: make(chan struct{}, cfg.MaxConcurrent)}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path != "/api/render" {
		s.writeError(w, failure(404, "not_found", "The endpoint does not exist."), "")
		return
	}
	expected := sha256.Sum256([]byte("Bearer " + s.cfg.APIKey))
	actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	if subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		s.writeError(w, failure(401, "unauthorized", "A valid bearer API key is required."), "")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		s.writeError(w, failure(405, "method_not_allowed", "Use POST /api/render."), "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(deadline)
	_ = controller.SetWriteDeadline(deadline)
	if ctx.Err() != nil {
		s.writeError(w, ctx.Err(), "")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.MaxRequestBytes)
	request, err := decodeRequest(r.Body)
	if err != nil {
		s.writeError(w, err, "")
		return
	}
	repo, err := s.cfg.Repository(request.RepoURL)
	if err != nil {
		s.writeError(w, err, "")
		return
	}
	if err := entrypointPath(request.Entrypoint); err != nil {
		s.writeError(w, err, "")
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		s.writeError(w, ctx.Err(), "")
		return
	default:
		w.Header().Set("Retry-After", "1")
		s.writeError(w, failure(429, "busy", "All render slots are busy; retry shortly."), "")
		return
	}
	start := time.Now()
	checkout, err := s.cache.Prepare(ctx, repo, request.Ref)
	if err != nil {
		s.writeError(w, err, "")
		return
	}
	defer func() {
		if err := checkout.Cleanup(); err != nil {
			s.log.Error("request cleanup failed", "code", "cleanup_error")
		}
	}()
	s.log.Info("project prepared", "commit", checkout.Commit, "duration_ms", time.Since(start).Milliseconds())
	pdf, err := Compile(ctx, s.cfg, checkout, request.Entrypoint, request.Data)
	if err != nil {
		s.writeError(w, err, checkout.RequestDir)
		return
	}
	if ctx.Err() != nil {
		s.writeError(w, ctx.Err(), checkout.RequestDir)
		return
	}
	file, err := os.Open(pdf)
	if err != nil {
		s.writeError(w, err, checkout.RequestDir)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		s.writeError(w, err, checkout.RequestDir)
		return
	}
	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", "attachment; filename=\"document.pdf\"")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("X-Source-Commit", checkout.Commit)
	w.WriteHeader(http.StatusOK)
	_, err = io.Copy(w, file)
	if err == nil {
		err = controller.Flush()
	}
	if err != nil {
		s.log.Info("PDF transfer ended early", "commit", checkout.Commit)
		return
	}
	s.log.Info("render completed", "commit", checkout.Commit, "duration_ms", time.Since(start).Milliseconds(), "pdf_bytes", info.Size())
}

func decodeRequest(body io.Reader) (RenderRequest, error) {
	var request RenderRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&request)
	if err == nil {
		var trailing any
		err = decoder.Decode(&trailing)
		if err == io.EOF {
			err = nil
		} else if err == nil {
			err = errors.New("multiple JSON values")
		}
	}
	if err != nil {
		var sizeErr *http.MaxBytesError
		if errors.As(err, &sizeErr) {
			return request, failure(413, "request_too_large", "The JSON request exceeds MAX_REQUEST_BYTES.")
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return request, context.DeadlineExceeded
		}
		return request, failure(400, "invalid_request", "Send one JSON object using repo_url, ref, entrypoint, and object data.")
	}
	if len(request.Data) == 0 {
		request.Data = json.RawMessage("{}")
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(bytes.TrimSpace(request.Data), &data); err != nil || data == nil {
		return request, failure(400, "invalid_request", "The data field must be a JSON object.")
	}
	if request.Ref == "" {
		request.Ref = "HEAD"
	}
	if request.Entrypoint == "" {
		request.Entrypoint = "main.typ"
	}
	return request, nil
}

func (s *Server) sanitize(err error, workPath string) *APIError {
	clean := *asAPIError(err)
	credentials := []string{s.cfg.APIKey, s.cfg.GiteaToken}
	if s.cfg.GiteaToken != "" {
		credentials = append(credentials,
			s.cfg.GiteaUsername+":"+s.cfg.GiteaToken,
			base64.StdEncoding.EncodeToString([]byte(s.cfg.GiteaUsername+":"+s.cfg.GiteaToken)))
	}
	for _, secret := range credentials {
		if secret != "" {
			clean.Diagnostics = strings.ReplaceAll(clean.Diagnostics, secret, "[redacted]")
			clean.Message = strings.ReplaceAll(clean.Message, secret, "[redacted]")
		}
	}
	for _, root := range []string{workPath, s.cfg.WorkDir, s.cfg.CacheDir} {
		if root != "" {
			clean.Diagnostics = strings.ReplaceAll(clean.Diagnostics, filepath.ToSlash(root), "<project>")
			clean.Diagnostics = strings.ReplaceAll(clean.Diagnostics, root, "<project>")
		}
	}
	if len(clean.Diagnostics) > diagnosticLimit {
		clean.Diagnostics = clean.Diagnostics[:diagnosticLimit]
	}
	return &clean
}

func (s *Server) writeError(w http.ResponseWriter, err error, workPath string) {
	// The computation deadline may already have expired. Give the small JSON
	// response a separate bounded window, so clients receive the timeout code.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second))
	apiErr := s.sanitize(err, workPath)
	body, _ := json.Marshal(apiErr)
	body = append(body, '\n')
	s.log.Info("render request failed", "code", apiErr.Code)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(apiErr.Status)
	_, _ = w.Write(body)
	// Cleanup may be waiting behind another request's Git operation. Explicit
	// length and flushing complete the response before that wait begins.
	_ = http.NewResponseController(w).Flush()
}
