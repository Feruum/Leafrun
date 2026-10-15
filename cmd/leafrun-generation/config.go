package main

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/Feruum/Leafrun/internal/orchestration"
	"github.com/Feruum/Leafrun/internal/service"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const (
	defaultHTTPAddr       = "127.0.0.1:8080"
	defaultTemporalNS     = "leafrun-generation"
	maxCredentialDocument = 64 << 10
)

type apiEnvironment struct {
	Runtime service.RuntimeConfig
	HTTP    string
}

func readAPIEnvironment(getenv func(string) string) (apiEnvironment, error) {
	var config apiEnvironment
	var err error
	config.Runtime.DatabaseURL, err = required(getenv, "GENERATION_DATABASE_URL")
	if err != nil {
		return config, err
	}
	config.Runtime.StorageURL, err = required(getenv, "GENERATION_STORAGE_URL")
	if err != nil {
		return config, err
	}
	config.Runtime.StorageToken, err = required(getenv, "GENERATION_STORAGE_TOKEN")
	if err != nil {
		return config, err
	}
	config.Runtime.AccessURL, err = required(getenv, "GENERATION_OWNER_URL")
	if err != nil {
		return config, err
	}
	config.Runtime.AccessToken, err = required(getenv, "GENERATION_OWNER_TOKEN")
	if err != nil {
		return config, err
	}
	config.Runtime.ProjectsURL = getenv("GENERATION_PROJECTS_URL")
	config.Runtime.ProjectsToken = getenv("GENERATION_PROJECTS_TOKEN")
	if (config.Runtime.ProjectsURL == "") != (config.Runtime.ProjectsToken == "") {
		return config, errors.New("GENERATION_PROJECTS_URL and GENERATION_PROJECTS_TOKEN must be configured together")
	}
	for _, value := range []string{config.Runtime.ProjectsURL, config.Runtime.ProjectsToken} {
		if strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
			return config, errors.New("Generation Projects dependency configuration is invalid")
		}
	}
	config.Runtime.OwnerCallbackToken, err = required(getenv, "GENERATION_OWNER_CALLBACK_TOKEN")
	if err != nil {
		return config, err
	}
	config.Runtime.EngineVersion, err = required(getenv, "GENERATION_ENGINE_VERSION")
	if err != nil {
		return config, err
	}
	if config.Runtime.EngineVersion != "0.15.1" {
		return config, errors.New("GENERATION_ENGINE_VERSION must be exactly 0.15.1")
	}
	credentialJSON, err := required(getenv, "GENERATION_CREDENTIALS_JSON")
	if err != nil {
		return config, err
	}
	config.Runtime.Credentials, err = parseCredentials(credentialJSON)
	if err != nil {
		return config, errors.New("GENERATION_CREDENTIALS_JSON is invalid")
	}
	if err = config.Runtime.ValidateCredentialSeparation(); err != nil {
		return config, err
	}
	config.HTTP = getenv("GENERATION_HTTP_ADDR")
	if config.HTTP == "" {
		config.HTTP = defaultHTTPAddr
	}
	if len(config.HTTP) > 256 || strings.ContainsAny(config.HTTP, "\r\n") {
		return config, errors.New("GENERATION_HTTP_ADDR is invalid")
	}
	if _, _, err = net.SplitHostPort(config.HTTP); err != nil {
		return config, errors.New("GENERATION_HTTP_ADDR must be a host:port address")
	}
	if queue := getenv("GENERATION_TASK_QUEUE"); queue != "" && queue != orchestration.TaskQueue {
		return config, fmt.Errorf("GENERATION_TASK_QUEUE is fixed to %s", orchestration.TaskQueue)
	}
	return config, nil
}

func readDatabaseURL(getenv func(string) string) (string, error) {
	return required(getenv, "GENERATION_DATABASE_URL")
}

func readTemporalEnvironment(getenv func(string) string) (string, string, error) {
	address, err := required(getenv, "GENERATION_TEMPORAL_ADDRESS")
	if err != nil {
		return "", "", err
	}
	if len(address) > 512 || strings.ContainsAny(address, " \t\r\n") {
		return "", "", errors.New("GENERATION_TEMPORAL_ADDRESS is invalid")
	}
	namespace := getenv("GENERATION_TEMPORAL_NAMESPACE")
	if namespace == "" {
		namespace = defaultTemporalNS
	}
	if len(namespace) > 128 || strings.TrimSpace(namespace) != namespace || strings.ContainsAny(namespace, "\x00\r\n") {
		return "", "", errors.New("GENERATION_TEMPORAL_NAMESPACE is invalid")
	}
	if queue := getenv("GENERATION_TASK_QUEUE"); queue != "" && queue != orchestration.TaskQueue {
		return "", "", fmt.Errorf("GENERATION_TASK_QUEUE is fixed to %s", orchestration.TaskQueue)
	}
	return address, namespace, nil
}

func required(getenv func(string) string, name string) (string, error) {
	value := getenv(name)
	if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n") {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func parseCredentials(raw string) (service.TokenResolver, error) {
	if len(raw) == 0 || len(raw) > maxCredentialDocument {
		return nil, errors.New("credential document size is invalid")
	}
	normalized, err := jsonvalue.NormalizeObject([]byte(raw))
	if err != nil {
		return nil, err
	}
	var tokens map[string]storagev1.Principal
	if err = jsonvalue.Unmarshal(normalized, &tokens, maxCredentialDocument); err != nil {
		return nil, err
	}
	if len(tokens) == 0 || len(tokens) > 1000 {
		return nil, errors.New("credential count is invalid")
	}
	for token, principal := range tokens {
		if token == "" || len(token) > 4096 || strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t\r\n,") {
			return nil, errors.New("credential token is invalid")
		}
		if _, err = service.EncodeInitiator(principal); err != nil {
			return nil, err
		}
	}
	return service.StaticTokens(tokens), nil
}
