package store

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/config"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/token"
)

var (
	ErrInvalidClient       = errors.New("invalid client credentials")
	ErrInvalidRefreshToken = errors.New("invalid refresh token")
	ErrRefreshReuse        = errors.New("refresh token reuse detected")
)

type Client struct {
	ID     string
	Scopes []string
}

type Store struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("create auth database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping auth database: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin auth database migration: %w", err)
	}
	defer tx.Rollback(ctx)
	// CREATE TABLE IF NOT EXISTS can still race on PostgreSQL system catalogs
	// when several replicas boot for the first time. Serialize this small,
	// idempotent migration across replicas.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(40310002)`); err != nil {
		return fmt.Errorf("lock auth database migration: %w", err)
	}
	statements := []string{`
		CREATE TABLE IF NOT EXISTS auth_clients (
			client_id TEXT PRIMARY KEY,
			secret_hash BYTEA NOT NULL,
			enabled BOOLEAN NOT NULL DEFAULT TRUE,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, `
		CREATE TABLE IF NOT EXISTS client_scopes (
			client_id TEXT NOT NULL REFERENCES auth_clients(client_id) ON DELETE CASCADE,
			scope TEXT NOT NULL,
			PRIMARY KEY (client_id, scope)
		)
	`, `
		CREATE TABLE IF NOT EXISTS refresh_sessions (
			session_id TEXT PRIMARY KEY,
			family_id TEXT NOT NULL,
			client_id TEXT NOT NULL REFERENCES auth_clients(client_id),
			token_hash BYTEA NOT NULL UNIQUE,
			expires_at TIMESTAMPTZ NOT NULL,
			used_at TIMESTAMPTZ,
			revoked_at TIMESTAMPTZ,
			replaced_by TEXT,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`, `CREATE INDEX IF NOT EXISTS refresh_sessions_family_idx ON refresh_sessions(family_id)`,
		`CREATE INDEX IF NOT EXISTS refresh_sessions_expiry_idx ON refresh_sessions(expires_at)`,
	}
	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return fmt.Errorf("migrate auth database: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit auth database migration: %w", err)
	}
	return nil
}

func (s *Store) SeedClients(ctx context.Context, clients []config.SeedClient) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin seed clients: %w", err)
	}
	defer tx.Rollback(ctx)
	// Multiple Auth replicas may start together. Serialize the idempotent seed
	// so delete-and-replace of the fixed scope set cannot race.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(40310003)`); err != nil {
		return fmt.Errorf("lock client seed: %w", err)
	}

	for _, client := range clients {
		secretHash := token.HashClientSecret(client.Secret)
		_, err := tx.Exec(ctx, `
			INSERT INTO auth_clients (client_id, secret_hash, enabled)
			VALUES ($1, $2, TRUE)
			ON CONFLICT (client_id) DO UPDATE
			SET secret_hash = EXCLUDED.secret_hash, enabled = TRUE, updated_at = NOW()
		`, client.ID, secretHash)
		if err != nil {
			return fmt.Errorf("upsert auth client %q: %w", client.ID, err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM client_scopes WHERE client_id = $1`, client.ID); err != nil {
			return fmt.Errorf("replace scopes for %q: %w", client.ID, err)
		}
		for _, scope := range client.Scopes {
			if _, err := tx.Exec(ctx, `INSERT INTO client_scopes (client_id, scope) VALUES ($1, $2)`, client.ID, scope); err != nil {
				return fmt.Errorf("insert scope for %q: %w", client.ID, err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit seed clients: %w", err)
	}
	return nil
}

func (s *Store) Authenticate(ctx context.Context, clientID, secret string) (Client, error) {
	var expectedHash []byte
	var enabled bool
	err := s.pool.QueryRow(ctx, `SELECT secret_hash, enabled FROM auth_clients WHERE client_id = $1`, clientID).Scan(&expectedHash, &enabled)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Client{}, ErrInvalidClient
		}
		return Client{}, fmt.Errorf("load auth client: %w", err)
	}
	providedHash := token.HashClientSecret(secret)
	if !enabled || subtle.ConstantTimeCompare(expectedHash, providedHash) != 1 {
		return Client{}, ErrInvalidClient
	}

	rows, err := s.pool.Query(ctx, `SELECT scope FROM client_scopes WHERE client_id = $1`, clientID)
	if err != nil {
		return Client{}, fmt.Errorf("load client scopes: %w", err)
	}
	defer rows.Close()
	scopes := make([]string, 0, 2)
	for rows.Next() {
		var scope string
		if err := rows.Scan(&scope); err != nil {
			return Client{}, fmt.Errorf("scan client scope: %w", err)
		}
		scopes = append(scopes, scope)
	}
	if err := rows.Err(); err != nil {
		return Client{}, fmt.Errorf("iterate client scopes: %w", err)
	}
	sort.Strings(scopes)
	return Client{ID: clientID, Scopes: scopes}, nil
}

func (s *Store) CreateRefreshSession(ctx context.Context, clientID string, expiresAt time.Time) (plainToken, sessionID string, err error) {
	plainToken, tokenHash, err := token.RandomOpaqueToken()
	if err != nil {
		return "", "", fmt.Errorf("generate refresh token: %w", err)
	}
	sessionID, err = token.RandomID()
	if err != nil {
		return "", "", fmt.Errorf("generate session ID: %w", err)
	}
	familyID, err := token.RandomID()
	if err != nil {
		return "", "", fmt.Errorf("generate token family ID: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO refresh_sessions (session_id, family_id, client_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, $5)
	`, sessionID, familyID, clientID, tokenHash, expiresAt.UTC())
	if err != nil {
		return "", "", fmt.Errorf("store refresh session: %w", err)
	}
	return plainToken, sessionID, nil
}

func (s *Store) RotateRefreshSession(ctx context.Context, clientID, oldPlainToken string, expiresAt, now time.Time) (plainToken, sessionID string, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", "", fmt.Errorf("begin refresh rotation: %w", err)
	}
	defer tx.Rollback(ctx)

	oldHash := token.HashOpaqueToken(oldPlainToken)
	var oldSessionID, familyID, ownerID string
	var oldExpiresAt time.Time
	var usedAt, revokedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT session_id, family_id, client_id, expires_at, used_at, revoked_at
		FROM refresh_sessions
		WHERE token_hash = $1
		FOR UPDATE
	`, oldHash).Scan(&oldSessionID, &familyID, &ownerID, &oldExpiresAt, &usedAt, &revokedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", "", ErrInvalidRefreshToken
		}
		return "", "", fmt.Errorf("load refresh session: %w", err)
	}

	now = now.UTC()
	if ownerID != clientID {
		return "", "", ErrInvalidRefreshToken
	}
	if usedAt != nil {
		if _, revokeErr := tx.Exec(ctx, `UPDATE refresh_sessions SET revoked_at = COALESCE(revoked_at, $2) WHERE family_id = $1`, familyID, now); revokeErr != nil {
			return "", "", fmt.Errorf("revoke reused token family: %w", revokeErr)
		}
		if err := tx.Commit(ctx); err != nil {
			return "", "", fmt.Errorf("commit reused token revocation: %w", err)
		}
		return "", "", ErrRefreshReuse
	}
	if revokedAt != nil || !now.Before(oldExpiresAt) {
		return "", "", ErrInvalidRefreshToken
	}

	plainToken, newHash, err := token.RandomOpaqueToken()
	if err != nil {
		return "", "", fmt.Errorf("generate replacement refresh token: %w", err)
	}
	sessionID, err = token.RandomID()
	if err != nil {
		return "", "", fmt.Errorf("generate replacement session ID: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO refresh_sessions (session_id, family_id, client_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4, $5)
	`, sessionID, familyID, clientID, newHash, expiresAt.UTC()); err != nil {
		return "", "", fmt.Errorf("insert replacement refresh session: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE refresh_sessions SET used_at = $2, replaced_by = $3 WHERE session_id = $1
	`, oldSessionID, now, sessionID); err != nil {
		return "", "", fmt.Errorf("consume old refresh session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", fmt.Errorf("commit refresh rotation: %w", err)
	}
	return plainToken, sessionID, nil
}
