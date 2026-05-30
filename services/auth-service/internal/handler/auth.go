package handler

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/thorOdinson16/multiplayer-infra/services/auth-service/internal/jwt"
	"github.com/thorOdinson16/multiplayer-infra/services/auth-service/internal/store"
	"golang.org/x/crypto/bcrypt"
)

// AuthHandler handles authentication HTTP requests
type AuthHandler struct {
	store      *store.CouchbaseStore
	jwtManager *jwt.Manager
	jwtExpiry  int
}

// NewAuthHandler creates a new auth handler
func NewAuthHandler(store *store.CouchbaseStore, jwtManager *jwt.Manager, jwtExpiryHours int) *AuthHandler {
	return &AuthHandler{
		store:      store,
		jwtManager: jwtManager,
		jwtExpiry:  jwtExpiryHours,
	}
}

// LoginRequest represents the login request body
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse represents the login response
type LoginResponse struct {
	Token     string `json:"token"`
	PlayerID  string `json:"playerId"`
	Username  string `json:"username"`
	ExpiresAt string `json:"expiresAt"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// Login handles POST /auth/login (FR-AUTH-01)
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "Only POST is allowed",
		})
		return
	}

	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_request",
			Message: "Invalid request body",
		})
		return
	}

	if req.Username == "" || req.Password == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "missing_fields",
			Message: "Username and password are required",
		})
		return
	}

	player, err := h.store.GetPlayerByUsername(req.Username)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, ErrorResponse{
			Error:   "invalid_credentials",
			Message: "Invalid username or password",
		})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(player.PasswordHash), []byte(req.Password)); err != nil {
		writeJSON(w, http.StatusUnauthorized, ErrorResponse{
			Error:   "invalid_credentials",
			Message: "Invalid username or password",
		})
		return
	}

	token, expiresAt, err := h.jwtManager.CreateToken(player.PlayerID, player.Username)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "token_error",
			Message: "Failed to create token",
		})
		return
	}

	session := &store.Session{
		PlayerID:  player.PlayerID,
		Token:     token,
		ExpiresAt: expiresAt.Format("2006-01-02T15:04:05Z"),
		IPAddress: r.RemoteAddr,
	}
	if err := h.store.CreateSession(session, h.jwtExpiry); err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "session_error",
			Message: "Failed to create session",
		})
		return
	}

	h.store.UpdateLastSeen(player.PlayerID)

	writeJSON(w, http.StatusOK, LoginResponse{
		Token:     token,
		PlayerID:  player.PlayerID,
		Username:  player.Username,
		ExpiresAt: expiresAt.Format("2006-01-02T15:04:05Z"),
	})
}

// Refresh handles POST /auth/refresh (FR-AUTH-05)
func (h *AuthHandler) Refresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "Only POST is allowed",
		})
		return
	}

	tokenString := extractToken(r)
	if tokenString == "" {
		writeJSON(w, http.StatusUnauthorized, ErrorResponse{
			Error:   "missing_token",
			Message: "Authorization token is required",
		})
		return
	}

	claims, err := h.jwtManager.ValidateToken(tokenString)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, ErrorResponse{
			Error:   "invalid_token",
			Message: "Token is invalid or expired",
		})
		return
	}

	newToken, expiresAt, err := h.jwtManager.CreateToken(claims.PlayerID, claims.Username)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "token_error",
			Message: "Failed to refresh token",
		})
		return
	}

	writeJSON(w, http.StatusOK, LoginResponse{
		Token:     newToken,
		PlayerID:  claims.PlayerID,
		Username:  claims.Username,
		ExpiresAt: expiresAt.Format("2006-01-02T15:04:05Z"),
	})
}

// Logout handles POST /auth/logout
func (h *AuthHandler) Logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error:   "method_not_allowed",
			Message: "Only POST is allowed",
		})
		return
	}

	var req struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.SessionID == "" {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_request",
			Message: "Session ID is required",
		})
		return
	}

	if err := h.store.DeleteSession(req.SessionID); err != nil {
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{
			Error:   "logout_error",
			Message: "Failed to logout",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "logged_out"})
}

// Validate handles GET /auth/validate
func (h *AuthHandler) Validate(w http.ResponseWriter, r *http.Request) {
	tokenString := extractToken(r)
	if tokenString == "" {
		writeJSON(w, http.StatusUnauthorized, ErrorResponse{
			Error:   "missing_token",
			Message: "Authorization token is required",
		})
		return
	}

	claims, err := h.jwtManager.ValidateToken(tokenString)
	if err != nil {
		status := http.StatusUnauthorized
		message := "Invalid token"
		if err == jwt.ErrExpiredToken {
			message = "Token has expired"
		}
		writeJSON(w, status, ErrorResponse{
			Error:   "invalid_token",
			Message: message,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"valid":    true,
		"playerId": claims.PlayerID,
		"username": claims.Username,
	})
}

// extractToken extracts the JWT from the Authorization header
func extractToken(r *http.Request) string {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}

	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return ""
	}

	return parts[1]
}