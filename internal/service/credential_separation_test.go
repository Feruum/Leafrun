package service

import (
	"context"
	"testing"

	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestOpenRuntimeRejectsPublicServiceCredentialsBeforeOpeningDatabase(t *testing.T) {
	for _, field := range []string{"projects", "storage", "callback", "access"} {
		config := RuntimeConfig{Credentials: StaticTokens{"public-service-token": storagev1.Principal{Subject: "user", Domain: "core"}}}
		switch field {
		case "projects":
			config.ProjectsToken = "public-service-token"
		case "storage":
			config.StorageToken = "public-service-token"
		case "callback":
			config.OwnerCallbackToken = "public-service-token"
		case "access":
			config.AccessToken = "public-service-token"
		}
		if _, err := OpenRuntime(context.Background(), config); err == nil || err.Error() != "public caller credentials must be distinct from service credentials" {
			t.Errorf("%s collision was not rejected at the trust boundary: %v", field, err)
		}
	}
}

func TestServerRejectsPublicOwnerCallbackCredential(t *testing.T) {
	_, err := New(newTestStore(), testOwner{}, Config{Credentials: StaticTokens{"owner-secret": {Subject: "user", Domain: "core"}}, Access: &testAccess{allowed: true}, Pins: &testPins{}, OwnerToken: "owner-secret", EngineVersion: "0.15.1"})
	if err == nil {
		t.Fatal("public token could authenticate private owner callbacks")
	}
}
