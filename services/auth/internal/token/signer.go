package token

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	ClientID  string `json:"client_id"`
	Scope     string `json:"scope"`
	SessionID string `json:"sid"`
	jwt.RegisteredClaims
}

type Signer struct {
	privateKey *rsa.PrivateKey
	issuer     string
	audience   string
	keyID      string
	ttl        time.Duration
}

func NewSigner(privateKeyPEM []byte, issuer, audience, keyID string, ttl time.Duration) (*Signer, error) {
	privateKey, err := parsePrivateKey(privateKeyPEM)
	if err != nil {
		return nil, err
	}
	if privateKey.N.BitLen() < 2048 {
		return nil, errors.New("RSA signing key must be at least 2048 bits")
	}
	if issuer == "" || audience == "" || keyID == "" || ttl <= 0 {
		return nil, errors.New("issuer, audience, key ID, and positive TTL are required")
	}
	return &Signer{privateKey: privateKey, issuer: issuer, audience: audience, keyID: keyID, ttl: ttl}, nil
}

func (s *Signer) Issue(clientID string, scopes []string, sessionID string, now time.Time) (string, error) {
	jti, err := randomID(18)
	if err != nil {
		return "", fmt.Errorf("generate access token ID: %w", err)
	}
	now = now.UTC()
	claims := Claims{
		ClientID:  clientID,
		Scope:     strings.Join(scopes, " "),
		SessionID: sessionID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.issuer,
			Subject:   clientID,
			Audience:  jwt.ClaimStrings{s.audience},
			ExpiresAt: jwt.NewNumericDate(now.Add(s.ttl)),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        jti,
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	t.Header["typ"] = "at+jwt"
	t.Header["kid"] = s.keyID
	return t.SignedString(s.privateKey)
}

func (s *Signer) JWKS() map[string]any {
	publicKey := s.privateKey.PublicKey
	e := big.NewInt(int64(publicKey.E)).Bytes()
	return map[string]any{
		"keys": []map[string]string{{
			"kty": "RSA",
			"use": "sig",
			"alg": "RS256",
			"kid": s.keyID,
			"n":   base64.RawURLEncoding.EncodeToString(publicKey.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(e),
		}},
	}
}

func (s *Signer) TTL() time.Duration { return s.ttl }

func parsePrivateKey(value []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(value)
	if block == nil {
		return nil, errors.New("JWT private key is not PEM")
	}
	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		rsaKey, ok := key.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("JWT private key is not RSA")
		}
		return rsaKey, rsaKey.Validate()
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse RSA private key: %w", err)
	}
	return key, key.Validate()
}

func RandomOpaqueToken() (plain string, hash []byte, err error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", nil, err
	}
	plain = base64.RawURLEncoding.EncodeToString(bytes)
	digest := sha256Bytes([]byte(plain))
	return plain, digest, nil
}

func HashOpaqueToken(value string) []byte { return sha256Bytes([]byte(value)) }

func HashClientSecret(value string) []byte { return sha256Bytes([]byte(value)) }

func sha256Bytes(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}

func RandomID() (string, error) { return randomID(18) }

func randomID(size int) (string, error) {
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(bytes), nil
}
