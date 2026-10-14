package service

import (
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const initiatorVersion = "initiator:v1:"

// EncodeInitiator stores the authenticated principal as a versioned opaque ID.
// The original bearer credential is never persisted.
func EncodeInitiator(principal storagev1.Principal) (string, error) {
	if !validPrincipalPart(principal.Subject) || !validPrincipalPart(principal.Domain) {
		return "", errors.New("principal subject and domain must be bounded nonempty identifiers")
	}
	encoded, err := json.Marshal(principal)
	if err != nil {
		return "", err
	}
	return initiatorVersion + base64.RawURLEncoding.EncodeToString(encoded), nil
}

// DecodeInitiator reverses EncodeInitiator without relying on a delimiter that
// could also occur in either principal field.
func DecodeInitiator(id string) (storagev1.Principal, error) {
	var principal storagev1.Principal
	if !strings.HasPrefix(id, initiatorVersion) {
		return principal, errors.New("unsupported initiator encoding")
	}
	payload := strings.TrimPrefix(id, initiatorVersion)
	decoded, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil || base64.RawURLEncoding.EncodeToString(decoded) != payload {
		return principal, errors.New("invalid initiator encoding")
	}
	if err = jsonvalue.Unmarshal(decoded, &principal, 1024); err != nil {
		return storagev1.Principal{}, fmt.Errorf("invalid initiator principal: %w", err)
	}
	if !validPrincipalPart(principal.Subject) || !validPrincipalPart(principal.Domain) {
		return storagev1.Principal{}, errors.New("invalid initiator principal")
	}
	return principal, nil
}

func validPrincipalPart(value string) bool {
	return len(value) > 0 && len(value) <= 256 && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\x00\r\n")
}

// TokenResolver resolves each request against current credential state.
// Implementations should not retain caller credentials after resolution.
type TokenResolver interface {
	ResolveToken(token string) (storagev1.Principal, bool)
}

// StaticTokens is a small explicit credential profile suitable for local
// controlled deployments. Keys are bearer tokens; values are stable principals.
type StaticTokens map[string]storagev1.Principal

func (credentials StaticTokens) ResolveToken(token string) (storagev1.Principal, bool) {
	if token == "" {
		return storagev1.Principal{}, false
	}
	for configured, principal := range credentials {
		if len(configured) == len(token) && subtle.ConstantTimeCompare([]byte(configured), []byte(token)) == 1 &&
			validPrincipalPart(principal.Subject) && validPrincipalPart(principal.Domain) {
			return principal, true
		}
	}
	return storagev1.Principal{}, false
}
