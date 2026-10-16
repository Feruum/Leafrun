package main

import (
	"context"
	"errors"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/Feruum/Leafrun/internal/service"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	"github.com/leafrun/leafrun-platform/events"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type relayEnvironment struct{ DatabaseURL, Address, User, Password string }

func readRelayEnvironment(getenv func(string) string) (relayEnvironment, error) {
	var out relayEnvironment
	var err error
	for _, field := range []struct {
		name   string
		target *string
	}{{"GENERATION_DATABASE_URL", &out.DatabaseURL}, {"GENERATION_NATS_ADDRESS", &out.Address}, {"GENERATION_NATS_USER", &out.User}, {"GENERATION_NATS_PASSWORD", &out.Password}} {
		*field.target, err = required(getenv, field.name)
		if err != nil {
			return out, err
		}
	}
	u, err := url.Parse(out.Address)
	if err != nil || (u.Scheme != "nats" && u.Scheme != "tls") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Path != "" || strings.ContainsAny(out.Address, " \t\r\n") {
		return relayEnvironment{}, errors.New("GENERATION_NATS_ADDRESS is invalid")
	}
	return out, nil
}

func runEventRelay(ctx context.Context, getenv func(string) string) error {
	config, err := readRelayEnvironment(getenv)
	if err != nil {
		return err
	}
	pool, _, err := service.OpenStore(ctx, config.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	connection, err := nats.Connect(config.Address, nats.UserInfo(config.User, config.Password), nats.CustomInboxPrefix("_INBOX.generation"), nats.Name("leafrun-generation-relay"), nats.Timeout(5*time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second))
	if err != nil {
		return errors.New("Generation event broker is unavailable")
	}
	defer connection.Close()
	broker, err := jetstream.New(connection)
	if err != nil {
		return errors.New("Generation JetStream client could not start")
	}
	publisher, err := events.NewOwnerAuditPublisher(events.JetStreamPublisher{Client: broker}, auditv1.ProducerGeneration, []string{events.GenerationCompletedSubject})
	if err != nil {
		return err
	}
	relay := events.Relay{DB: pool, Publisher: publisher}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for ctx.Err() == nil {
		call, cancel := context.WithTimeout(ctx, 10*time.Second)
		count, err := relay.RunOnce(call)
		cancel()
		if err != nil && ctx.Err() == nil {
			log.Print("Generation outbox relay will retry a failed iteration")
		}
		if count > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
	return nil
}
