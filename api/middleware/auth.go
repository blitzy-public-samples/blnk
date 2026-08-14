/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package middleware

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/logsafe"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

const (
	KeyHeader = "X-Blnk-Key"

	// lastUsedUpdateBudget bounds the background last-used write.
	lastUsedUpdateBudget = 5 * time.Second
)

// abortWithCode writes the standard dual error payload (legacy flat "error"
// string + structured "error_detail") and aborts the request. The status is
// resolved from the error-code catalog.
func abortWithCode(c *gin.Context, code apierror.ErrorCode, message string) {
	resp := apierror.NewErrorResponse(code, message, nil)
	c.AbortWithStatusJSON(apierror.StatusForCode(resp.Error.Code), gin.H{
		"error":        message,
		"error_detail": resp.Error,
	})
}

// pathToResource maps URL paths to their corresponding resource types.
var pathToResource = map[string]Resource{
	"ledgers":          ResourceLedgers,
	"balances":         ResourceBalances,
	"accounts":         ResourceAccounts,
	"identities":       ResourceIdentities,
	"transactions":     ResourceTransactions,
	"balance-monitors": ResourceBalanceMonitors,
	"hooks":            ResourceHooks,
	"api-keys":         ResourceAPIKeys,
	"search":           ResourceSearch,
	"reconciliation":   ResourceReconciliation,
	"metadata":         ResourceMetadata,
	"backup":           ResourceBackup,
	"events":           ResourceEvents,
	"subscribers":      ResourceSubscribers,
}

// AuthMiddleware handles authentication and authorization for API routes.
type AuthMiddleware struct {
	service *blnk.Blnk
}

// NewAuthMiddleware creates a new instance of AuthMiddleware.
//
// Parameters:
// - blnk: The Blnk service used to validate API keys.
//
// Returns:
// - *AuthMiddleware: A new instance of the authentication middleware.
func NewAuthMiddleware(blnk *blnk.Blnk) *AuthMiddleware {
	return &AuthMiddleware{service: blnk}
}

// getResourceFromPath determines the resource type from the URL path.
func getResourceFromPath(path string) Resource {
	// Remove leading slash and get first path segment
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}

	// Special case for mocked-account
	if parts[0] == "mocked-account" {
		return ResourceAccounts
	}

	// Special case for multi-search
	if parts[0] == "multi-search" {
		return ResourceSearch
	}

	if parts[0] == "refund-transaction" {
		return ResourceTransactions
	}

	// Check if the path segment maps to a known resource
	if resource, ok := pathToResource[parts[0]]; ok {
		return resource
	}

	return ""
}

// injectAPIKeyToMetadata modifies the request body to include the API key ID in the meta_data.
func injectAPIKeyToMetadata(c *gin.Context, apiKeyID string) error {
	// Only proceed if this is a POST request
	if c.Request.Method != "POST" {
		return nil
	}

	// Check if the request body is nil
	if c.Request.Body == nil {
		return nil
	}

	// Read the request body
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return err
	}
	_ = c.Request.Body.Close()

	// Parse the request body as JSON
	var bodyMap map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &bodyMap); err != nil {
		// If not valid JSON, restore the original body and continue
		c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		return err
	}

	// Check if meta_data field exists
	metaData, ok := bodyMap["meta_data"].(map[string]interface{})
	if !ok {
		// If meta_data doesn't exist or is not an object, create it
		metaData = make(map[string]interface{})
	}

	// Add the API key ID to meta_data
	metaData["BLNK_GENERATED_BY"] = apiKeyID

	// Update the meta_data in the body
	bodyMap["meta_data"] = metaData

	// Convert back to JSON
	modifiedBody, err := json.Marshal(bodyMap)
	if err != nil {
		// If marshaling fails, restore the original body and continue
		c.Request.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		return err
	}

	// Set the modified body back to the request
	c.Request.Body = io.NopCloser(bytes.NewBuffer(modifiedBody))
	c.Request.ContentLength = int64(len(modifiedBody))

	return nil
}

// Authenticate returns a middleware function that handles authentication and authorization for all routes.
//
// Returns:
// - gin.HandlerFunc: A middleware function that performs the authentication.
func (m *AuthMiddleware) Authenticate() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip auth for root path
		if c.Request != nil && c.Request.URL != nil && c.Request.URL.Path == "/" {
			c.Next()
			return
		}

		// Skip auth for health endpoint (server health check only)
		if c.Request != nil && c.Request.URL != nil && c.Request.URL.Path == "/health" {
			c.Next()
			return
		}

		// Skip X-Blnk-Key auth for metrics endpoint, it uses its own bearer token auth
		if c.Request != nil && c.Request.URL != nil && c.Request.URL.Path == "/metrics" {
			c.Next()
			return
		}

		// AND FOR THE PROFILING SURFACE, for the same reason and behind the same credential.
		// Skipping X-Blnk-Key here is not opening the surface: /debug/pprof is registered with
		// MetricsAuth, which is the one that actually admits or refuses, and it is deliberately
		// the SAME middleware the metrics endpoint uses. Without this branch the profiles would
		// answer 401 AUTH_MISSING_API_KEY to a caller holding the metrics bearer token, which is
		// the credential the runbook tells an operator to use — so the surface would exist and
		// be unreachable exactly when it is needed.
		if c.Request != nil && c.Request.URL != nil &&
			strings.HasPrefix(c.Request.URL.Path, "/debug/pprof") {
			c.Next()
			return
		}

		// Check if secure mode is enabled
		conf, err := config.Fetch()
		if err == nil && conf != nil && !conf.Server.Secure {
			// Skip authentication when secure mode is disabled
			c.Next()
			return
		}

		key := extractKey(c)
		if key == "" {
			abortWithCode(c, apierror.ErrAuthMissingAPIKey, "Authentication required. Use X-Blnk-Key header")
			return
		}

		// First check if it's the master key
		if err == nil && conf != nil && conf.Server.SecretKey != "" &&
			subtle.ConstantTimeCompare([]byte(conf.Server.SecretKey), []byte(key)) == 1 {
			// Master key has all permissions
			c.Set("isMasterKey", true)
			c.Next()
			return
		}

		// If not master key, try API key authentication
		apiKey, err := m.service.GetAPIKeyByKey(c.Request.Context(), key)
		if err != nil {
			abortWithCode(c, apierror.ErrAuthInvalidAPIKey, "Invalid API key")
			return
		}

		if !apiKey.IsValid() {
			abortWithCode(c, apierror.ErrAuthExpiredAPIKey, "API key is expired or revoked")
			return
		}

		// Determine required resource from path
		if c.Request == nil || c.Request.URL == nil {
			abortWithCode(c, apierror.ErrGenInternal, "Invalid request")
			return
		}

		resource := getResourceFromPath(c.Request.URL.Path)
		if resource == "" {
			abortWithCode(c, apierror.ErrAuthUnknownResource, "Unknown resource type")
			return
		}

		// Check if API key has permission for this resource and method
		if !HasPermission(apiKey.Scopes, resource, c.Request.Method) {
			// Get the required action for this method
			action := methodToAction[c.Request.Method]
			abortWithCode(c, apierror.ErrAuthInsufficientPermissions, "Insufficient permissions for "+string(resource)+":"+string(action))
			return
		}

		// For POST requests, inject the API key ID into the metadata
		if c.Request.Method == "POST" && c.Request.Body != nil {
			if err := injectAPIKeyToMetadata(c, apiKey.APIKeyID); err != nil {
				// The cause is sanitized, bounded and redacted rather than concatenated raw: this
				// runs on an authenticated request, and the error comes from reading and rewriting
				// a caller-supplied body, so its text is partly the caller's own. See
				// internal/logsafe.
				logrus.WithField("cause", logsafe.Cause(err)).
					Error("failed to inject the API key id into the request metadata")
			}
		}

		// UPDATE THE LAST-USED TIMESTAMP OFF THE RESPONSE PATH.
		requestCtx := c.Request.Context()
		apiKeyID := apiKey.APIKeyID

		go func() {
			ctx, cancel := context.WithTimeout(
				context.WithoutCancel(requestCtx), lastUsedUpdateBudget)
			defer cancel()

			if err := m.service.UpdateLastUsed(ctx, apiKeyID); err != nil {
				logrus.WithField("cause", logsafe.Cause(err)).
					Debug("failed to update the API key's last-used timestamp")
			}
		}()

		c.Set("apiKey", apiKey)
		c.Next()
	}
}

// extractKey retrieves the authentication key from the X-Blnk-Key header.
func extractKey(c *gin.Context) string {
	return c.GetHeader(KeyHeader)
}

// MasterKeyRequest reports whether this request is authenticated as the deployment's
// MASTER KEY — the credential the privileged management surfaces are gated on.
//
// Authenticate sets the "isMasterKey" context value when it recognises the master key, so
// that value is authoritative WHENEVER IT IS PRESENT: a middleware that has already
// decided must not be second-guessed, and a test that sets it deliberately must keep
// deciding.
//
// THE FALLBACK EXISTS BECAUSE THERE IS A PATH ON WHICH NOTHING DECIDES. With secure mode
// off, Authenticate returns before comparing anything — authentication is disabled, so
// every route is open — and the context value is therefore never set. The management
// handlers read it and refused, which made the entire /events and /subscribers surface
// answer 403 AUTH_MASTER_KEY_REQUIRED on the shipped Compose default, with the correct
// master key presented, and left the dead-letter triage and daily reconciliation runbooks
// unexecutable on a local stack. That is a reachability defect rather than a security
// property: the response was the same for the master key and for no key at all.
//
// So when nothing has decided, the presented key is compared against the configured
// master key with the SAME constant-time comparison Authenticate uses. This cannot widen
// access anywhere:
//
//   - It only ever returns true for a caller that presents the configured master key, so
//     it can only agree with the comparison Authenticate would have made.
//   - It stays FAIL-CLOSED when no key is presented, when no master key is configured, or
//     when the key does not match — a caller without the credential is refused in insecure
//     mode exactly as it is in secure mode.
//
// Parameters:
//   - c *gin.Context: the request. A nil context, or one with no request, is not a master
//     key request.
//
// Returns:
//   - bool: true only for a request carrying the deployment's master key.
func MasterKeyRequest(c *gin.Context) bool {
	if c == nil {
		return false
	}

	if value, decided := c.Get("isMasterKey"); decided {
		isMaster, _ := value.(bool)

		return isMaster
	}

	if c.Request == nil {
		return false
	}

	// Read through the same accessor Authenticate uses, so the header this compares is the
	// header the middleware would have compared.
	presented := extractKey(c)
	if presented == "" {
		return false
	}

	conf, err := config.Fetch()
	if err != nil || conf == nil || conf.Server.SecretKey == "" {
		// No configured master key means no request can be one. A deployment that has not set
		// BLNK_SERVER_SECRET_KEY has no master credential to present, and treating an
		// unconfigured secret as a match would open the surface to any caller at all.
		return false
	}

	return subtle.ConstantTimeCompare([]byte(conf.Server.SecretKey), []byte(presented)) == 1
}
