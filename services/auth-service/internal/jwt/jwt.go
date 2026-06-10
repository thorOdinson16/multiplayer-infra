package jwt

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	golangjwt "github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
)

// Manager handles JWT creation and validation
type Manager struct {
	secret     []byte
	expiryTime time.Duration
}

// Claims represents the JWT claims
type Claims struct {
	PlayerID string `json:"playerId"`
	Username string `json:"username"`
	golangjwt.RegisteredClaims
}

// NewManager creates a new JWT manager
func NewManager(secret string, expiryHours int) *Manager {
	return &Manager{
		secret:     []byte(secret),
		expiryTime: time.Duration(expiryHours) * time.Hour,
	}
}

// GenerateSecret creates a random 32-byte hex-encoded secret
func GenerateSecret() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// CreateToken generates a signed JWT for a player
func (m *Manager) CreateToken(playerID, username string) (string, time.Time, error) {
	now := time.Now()
	expiresAt := now.Add(m.expiryTime)

	claims := &Claims{
		PlayerID: playerID,
		Username: username,
		RegisteredClaims: golangjwt.RegisteredClaims{
			ExpiresAt: golangjwt.NewNumericDate(expiresAt),
			IssuedAt:  golangjwt.NewNumericDate(now),
			Issuer:    "multiplayer-infra",
			Subject:   playerID,
		},
	}

	token := golangjwt.NewWithClaims(golangjwt.SigningMethodHS256, claims)
	signedToken, err := token.SignedString(m.secret)
	if err != nil {
		return "", time.Time{}, err
	}

	return signedToken, expiresAt, nil
}

// ValidateToken validates a JWT and returns the claims
func (m *Manager) ValidateToken(tokenString string) (*Claims, error) {
	token, err := golangjwt.ParseWithClaims(tokenString, &Claims{}, func(token *golangjwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*golangjwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidToken
		}
		return m.secret, nil
	})

	if err != nil {
		if errors.Is(err, golangjwt.ErrTokenExpired) {
			return nil, ErrExpiredToken
		}
		return nil, ErrInvalidToken
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}
