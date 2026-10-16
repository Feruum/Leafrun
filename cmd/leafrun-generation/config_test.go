package main

import (
	"testing"

	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func env(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestReadAPIEnvironmentRequiresPinnedAndExplicitSecurityConfig(t *testing.T) {
	values := map[string]string{
		"LEAFRUN_GUEST_COOKIE_NAME": "custom_guest", "GENERATION_SESSION_COOKIE": "custom_human", "GENERATION_AUTH_MODE": "dev", "GENERATION_DEV_FIXTURES": "true", "GENERATION_DATABASE_URL": "postgres://user:secret@db/generation",
		"GENERATION_STORAGE_URL":          "http://storage:8080",
		"GENERATION_STORAGE_TOKEN":        "storage-secret",
		"GENERATION_OWNER_URL":            "http://access:8080",
		"GENERATION_OWNER_TOKEN":          "access-secret",
		"GENERATION_OWNER_CALLBACK_TOKEN": "callback-secret",
		"GENERATION_ENGINE_VERSION":       "0.15.1",
		"GENERATION_CREDENTIALS_JSON":     `{"caller-token":{"subject":"user-1","domain":"core"}}`,
	}
	config, err := readAPIEnvironment(env(values))
	if err != nil {
		t.Fatal(err)
	}
	if config.Runtime.GuestCookieName != "custom_guest" || config.Runtime.CookieName != "custom_human" {
		t.Fatal("configured cookie names lost")
	}
	if config.HTTP != defaultHTTPAddr {
		t.Fatalf("default HTTP address = %q", config.HTTP)
	}
	principal, ok := config.Runtime.Credentials.ResolveToken("caller-token")
	if !ok || principal != (storagev1.Principal{Subject: "user-1", Domain: "core"}) {
		t.Fatalf("credential resolver = %#v, %v", principal, ok)
	}
	for _, field := range []string{"GENERATION_OWNER_TOKEN", "GENERATION_OWNER_CALLBACK_TOKEN", "GENERATION_CREDENTIALS_JSON"} {
		missing := make(map[string]string, len(values))
		for key, value := range values {
			missing[key] = value
		}
		delete(missing, field)
		if _, err = readAPIEnvironment(env(missing)); err == nil {
			t.Fatalf("missing %s was accepted", field)
		}
	}
}

func TestReadAPIEnvironmentRejectsQueueAndEngineDrift(t *testing.T) {
	values := map[string]string{
		"GENERATION_AUTH_MODE": "dev", "GENERATION_DEV_FIXTURES": "true", "GENERATION_DATABASE_URL": "postgres://user:secret@db/generation",
		"GENERATION_STORAGE_URL":          "http://storage:8080",
		"GENERATION_STORAGE_TOKEN":        "storage-secret",
		"GENERATION_OWNER_URL":            "http://access:8080",
		"GENERATION_OWNER_TOKEN":          "access-secret",
		"GENERATION_OWNER_CALLBACK_TOKEN": "callback-secret",
		"GENERATION_ENGINE_VERSION":       "0.15.1",
		"GENERATION_CREDENTIALS_JSON":     `{"token":{"subject":"user-1","domain":"core"}}`,
	}
	values["GENERATION_TASK_QUEUE"] = "other-queue"
	if _, err := readAPIEnvironment(env(values)); err == nil {
		t.Fatal("custom Temporal task queue was accepted")
	}
	delete(values, "GENERATION_TASK_QUEUE")
	values["GENERATION_ENGINE_VERSION"] = "0.15.2"
	if _, err := readAPIEnvironment(env(values)); err == nil {
		t.Fatal("engine version drift was accepted")
	}
}

func TestParseCredentialsRejectsDuplicateAndUnknownFields(t *testing.T) {
	for _, raw := range []string{
		`{"token":{"subject":"a","domain":"b"},"token":{"subject":"c","domain":"d"}}`,
		`{"token":{"subject":"a","domain":"b","role":"admin"}}`,
		`{"token":{"subject":" ","domain":"b"}}`,
	} {
		if _, err := parseCredentials(raw); err == nil {
			t.Fatalf("invalid credentials accepted: %s", raw)
		}
	}
}

func TestReadTemporalEnvironmentUsesIsolatedDefaultNamespace(t *testing.T) {
	address, namespace, err := readTemporalEnvironment(env(map[string]string{"GENERATION_TEMPORAL_ADDRESS": "temporal:7233"}))
	if err != nil {
		t.Fatal(err)
	}
	if address != "temporal:7233" || namespace != defaultTemporalNS || namespace == "default" {
		t.Fatalf("Temporal settings = %q %q", address, namespace)
	}
}

func TestAPIEnvironmentRejectsPublicCredentialMatchingFixedServiceTokens(t *testing.T) {
	for _, field := range []string{"GENERATION_PROJECTS_TOKEN", "GENERATION_STORAGE_TOKEN", "GENERATION_OWNER_CALLBACK_TOKEN", "GENERATION_OWNER_TOKEN"} {
		values := map[string]string{
			"GENERATION_AUTH_MODE": "dev", "GENERATION_DEV_FIXTURES": "true", "GENERATION_DATABASE_URL": "postgres://generation@db/generation", "GENERATION_STORAGE_URL": "http://storage:8081", "GENERATION_STORAGE_TOKEN": "storage-secret",
			"GENERATION_OWNER_URL": "http://access:8082", "GENERATION_OWNER_TOKEN": "access-secret", "GENERATION_OWNER_CALLBACK_TOKEN": "callback-secret",
			"GENERATION_ENGINE_VERSION": "0.15.1", "GENERATION_CREDENTIALS_JSON": `{"public-service-token":{"subject":"user","domain":"core"}}`,
		}
		values[field] = "public-service-token"
		if field == "GENERATION_PROJECTS_TOKEN" {
			values["GENERATION_PROJECTS_URL"] = "http://projects:8084"
		}
		if _, err := readAPIEnvironment(env(values)); err == nil {
			t.Errorf("%s could be used by a public caller", field)
		}
	}
}
