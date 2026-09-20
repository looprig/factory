// Package wiring demonstrates the storage and admission portion of a product
// composition root. A product supplies listeners, auth, Host targets and
// lifecycle; this package does not start a Factory server.
package wiring

import (
	"context"
	"fmt"
	"time"

	"github.com/looprig/factory"
	"github.com/looprig/pgstore"
	"github.com/looprig/s3store"
	"github.com/looprig/sessionstore"
	"github.com/looprig/storage"
)

// Config is populated by the product's secret and configuration provider.
// No raw credentials should appear in a manifest or be logged on error.
type Config struct {
	PostgresDSN      string // sslmode=verify-full with a trusted server CA
	S3Endpoint       string // HTTPS only
	S3Region         string
	S3Bucket         string
	DeploymentPrefix string // shared by all replicas, canonical Storage name
	KMSKeyID         string
}

// Open owns the PostgreSQL pool and the SessionStore. Close the SessionStore
// before the returned PostgreSQL pool after Factory and Host have stopped.
// Open calls require a bounded context; the caller should use a startup timeout.
func Open(ctx context.Context, cfg Config) (*sessionstore.Store, *pgstore.Store, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, nil, fmt.Errorf("deployment wiring: startup context needs a deadline")
	}
	pg, err := pgstore.Open(ctx, pgstore.Options{
		DSN:              cfg.PostgresDSN,
		MaxConns:         10,
		StatementTimeout: 10 * time.Second,
		LockTimeout:      5 * time.Second,
		Migrations:       pgstore.MigrationValidate,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("open structured storage: %w", err)
	}
	blobs, err := s3store.Open(ctx, s3store.Options{
		Endpoint:                   cfg.S3Endpoint,
		Region:                     cfg.S3Region,
		Bucket:                     cfg.S3Bucket,
		DeploymentPrefix:           cfg.DeploymentPrefix,
		Encryption:                 s3store.EncryptionKMS,
		KMSKeyID:                   cfg.KMSKeyID,
		RequireConfirmedEncryption: true,
		MaxConcurrentTransfers:     4,
	})
	if err != nil {
		pg.Close()
		return nil, nil, fmt.Errorf("open object storage: %w", err)
	}
	backend, err := storage.NewCompositeWithOrderedIndex(pg.Ledger, pg.Leaser, pg.KV, blobs, pg.OrderedIndex)
	if err != nil {
		pg.Close()
		return nil, nil, fmt.Errorf("compose storage: %w", err)
	}
	store, err := sessionstore.Open(ctx, backend)
	if err != nil {
		pg.Close()
		return nil, nil, fmt.Errorf("open session store: %w", err)
	}
	return store, pg, nil
}

// ClientLimits is the explicit per-replica admission/queue policy.
func ClientLimits(maxConnections int) (factory.ClientLinkLimits, error) {
	limits := factory.DefaultClientLinkLimits()
	limits.MaxConnections = maxConnections
	limits.MaxChannelsPerConnection = 256
	limits.PerConnectionQueueBytes = 1 << 20
	if err := limits.Validate(); err != nil {
		return factory.ClientLinkLimits{}, err
	}
	return limits, nil
}
