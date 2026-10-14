package main

import "testing"

func TestRelayConfigurationKeepsCredentialOutOfAddress(t *testing.T) {
	values := map[string]string{"GENERATION_DATABASE_URL": "postgres://generation:private@localhost/generation", "GENERATION_NATS_ADDRESS": "nats://localhost:4222", "GENERATION_NATS_USER": "generation", "GENERATION_NATS_PASSWORD": "private"}
	get := func(k string) string { return values[k] }
	if _, err := readRelayEnvironment(get); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"nats://name:secret@localhost:4222", "https://localhost:4222", "nats://localhost:4222?secret=hidden", "nats://localhost:4222/#private"} {
		values["GENERATION_NATS_ADDRESS"] = address
		if _, err := readRelayEnvironment(get); err == nil {
			t.Fatalf("accepted address %q", address)
		}
	}
	values["GENERATION_NATS_ADDRESS"] = "nats://localhost:4222"
	delete(values, "GENERATION_NATS_PASSWORD")
	if _, err := readRelayEnvironment(get); err == nil {
		t.Fatal("missing credential accepted")
	}
}
