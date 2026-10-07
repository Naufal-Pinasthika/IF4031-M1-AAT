package service

import (
	"context"
	"strings"
	"time"

	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/store"
	"github.com/Naufal-Pinasthika/IIF4031-M1-AAT/services/auth/internal/token"
)

type Service struct {
	store      *store.Store
	signer     *token.Signer
	refreshTTL time.Duration
	now        func() time.Time
}

type TokenPair struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	Scope        string `json:"scope"`
}

func New(store *store.Store, signer *token.Signer, refreshTTL time.Duration) *Service {
	return &Service{store: store, signer: signer, refreshTTL: refreshTTL, now: time.Now}
}

func (s *Service) Authenticate(ctx context.Context, clientID, secret string) (store.Client, error) {
	return s.store.Authenticate(ctx, clientID, secret)
}

func (s *Service) Issue(ctx context.Context, client store.Client) (TokenPair, error) {
	now := s.now().UTC()
	refreshToken, sessionID, err := s.store.CreateRefreshSession(ctx, client.ID, now.Add(s.refreshTTL))
	if err != nil {
		return TokenPair{}, err
	}
	accessToken, err := s.signer.Issue(client.ID, client.Scopes, sessionID, now)
	if err != nil {
		return TokenPair{}, err
	}
	return newPair(accessToken, refreshToken, client.Scopes, s.signer.TTL()), nil
}

func (s *Service) Refresh(ctx context.Context, client store.Client, oldRefreshToken string) (TokenPair, error) {
	now := s.now().UTC()
	refreshToken, sessionID, err := s.store.RotateRefreshSession(ctx, client.ID, oldRefreshToken, now.Add(s.refreshTTL), now)
	if err != nil {
		return TokenPair{}, err
	}
	accessToken, err := s.signer.Issue(client.ID, client.Scopes, sessionID, now)
	if err != nil {
		return TokenPair{}, err
	}
	return newPair(accessToken, refreshToken, client.Scopes, s.signer.TTL()), nil
}

func newPair(accessToken, refreshToken string, scopes []string, ttl time.Duration) TokenPair {
	return TokenPair{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		TokenType:    "Bearer",
		ExpiresIn:    int64(ttl / time.Second),
		Scope:        strings.Join(scopes, " "),
	}
}
