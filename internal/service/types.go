package service

import (
	"context"
	"errors"
	"time"

	"github.com/Feruum/Leafrun/internal/store"
	generationv1 "github.com/leafrun/leafrun-contracts/generation/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

// AdmissionStore is the durable state boundary used by the Generation HTTP API
// and admission reconciler. Implementations must persist pin requests before
// callers invoke Storage.
type AdmissionStore interface {
	LookupAdmission(context.Context, string, string, string, string) (generationv1.Admission, bool, error)
	Admit(context.Context, store.AdmitInput) (generationv1.Admission, bool, error)
	GetJob(context.Context, string) (generationv1.Job, error)
	RequestCancel(context.Context, string) (bool, error)
	LatestPinAcquire(context.Context, string, string) (store.PinAcquire, error)
	RecordPinAcquireExpired(context.Context, string, string) error
	AdmissionActivated(context.Context, string) (bool, error)
	PendingAdmissions(context.Context, int) ([]string, error)
	BeginPinAcquireIntent(context.Context, store.PinAcquireIntent) (store.PinAcquire, error)
	RecordPinAcquireResponse(context.Context, string, string, storagev1.Pin) error
	MarkPinActiveAndRecordWorkflowStart(context.Context, string, string, string, storagev1.Pin, []byte) error
	MarkPinActive(context.Context, string, string, storagev1.Pin) error
	ExpireDue(context.Context, int) (int, error)
	Retain(context.Context) (store.RetentionResult, error)
}

// OwnerStore is Generation's owner callback surface for Storage.
type OwnerStore interface {
	Publishable(context.Context, storagev1.PublishabilityCheck) (bool, error)
	Pin(context.Context, storagev1.PinOwnerCheck) (storagev1.OwnerState, error)
}

// AccessChecker performs a current, authoritative access decision on every
// request. Errors are dependency failures and must fail closed.
type AccessChecker interface {
	Check(context.Context, storagev1.AccessCheck) (bool, error)
}

// PinStorage is the Storage pin API required for source snapshot admission.
type PinStorage interface {
	GetSnapshot(context.Context, string) (storagev1.Snapshot, error)
	AcquirePin(context.Context, storagev1.AcquirePinRequest) (storagev1.Pin, error)
	GetPin(context.Context, string) (storagev1.Pin, error)
	ActivatePin(context.Context, string) (storagev1.Pin, error)
}

type Config struct {
	Credentials   TokenResolver
	Access        AccessChecker
	Pins          PinStorage
	OwnerToken    string
	EngineVersion string
	// Ready must verify the durable dependencies required to serve requests.
	// A missing callback keeps /readyz fail-closed.
	Ready func(context.Context) error
	Now   func() time.Time
}

func (c Config) validate() error {
	if c.Credentials == nil || c.Access == nil || c.Pins == nil || c.OwnerToken == "" {
		return errors.New("credentials, Access, Storage pins and the owner callback token are required")
	}
	if c.EngineVersion != "0.15.1" {
		return errors.New("Generation requires the pinned Typst engine version 0.15.1")
	}
	return nil
}
