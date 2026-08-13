package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupBlnk() (*blnk.Blnk, error) {
	config.MockConfig(&config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})
	cnf, err := config.Fetch()
	if err != nil {
		return nil, err
	}
	db, err := database.NewDataSource(cnf)
	if err != nil {
		return nil, err
	}

	return blnk.NewBlnk(db)
}

func TestAuthMiddleware_Authenticate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Setup blnk service
	blnkService, err := setupBlnk()
	assert.NoError(t, err)

	// Create test API keys
	validKey, err := blnkService.CreateAPIKey(context.Background(), "valid-key", "test-owner", []string{"ledgers:read"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	insufficientKey, err := blnkService.CreateAPIKey(context.Background(), "insufficient-key", "test-owner", []string{"ledgers:read"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	expiredKey, err := blnkService.CreateAPIKey(context.Background(), "expired-key", "test-owner", []string{"ledgers:read"}, time.Now().Add(-24*time.Hour))
	assert.NoError(t, err)

	revokedKey, err := blnkService.CreateAPIKey(context.Background(), "revoked-key", "test-owner", []string{"read"}, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	err = blnkService.RevokeAPIKey(context.Background(), revokedKey.APIKeyID, revokedKey.OwnerID)
	assert.NoError(t, err)

	allPermissionsScopes := []string{
		"ledgers:read", "ledgers:write", "ledgers:delete",
		"balances:read", "balances:write", "balances:delete",
		"accounts:read", "accounts:write", "accounts:delete",
		"identities:read", "identities:write", "identities:delete",
		"transactions:read", "transactions:write", "transactions:delete",
		"balance-monitors:read", "balance-monitors:write", "balance-monitors:delete",
		"hooks:read", "hooks:write", "hooks:delete",
		"api-keys:read", "api-keys:write", "api-keys:delete",
		"search:read", "search:write", "search:delete",
		"reconciliation:read", "reconciliation:write", "reconciliation:delete",
		"metadata:read", "metadata:write", "metadata:delete",
		"backup:read", "backup:write", "backup:delete",
	}

	comprehensiveKey, err := blnkService.CreateAPIKey(context.Background(), "comprehensive-key", "test-owner", allPermissionsScopes, time.Now().Add(24*time.Hour))
	assert.NoError(t, err)

	tests := []struct {
		name          string
		path          string
		method        string
		apiKey        string
		expectedCode  int
		expectedError string
		setupConfig   func() *config.Configuration
	}{
		{
			name:   "Valid master key",
			path:   "/ledgers",
			method: "GET",
			apiKey: "master-key",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure:    true,
						SecretKey: "master-key",
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Root path",
			path:   "/",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Server health endpoint",
			path:   "/health",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Insufficient permissions",
			path:   "/ledgers",
			method: "POST",
			apiKey: insufficientKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusForbidden,
			expectedError: "Insufficient permissions for ledgers:write",
		},
		{
			name:   "Secure mode disabled",
			path:   "/ledgers",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: false,
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Unknown resource type",
			path:   "/fake-resouce/dd",
			method: "POST",
			apiKey: validKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode: http.StatusForbidden,
		},
		{
			name:   "Valid API key with read permission",
			path:   "/ledgers",
			method: "GET",
			apiKey: validKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode: http.StatusOK,
		},
		{
			name:   "Invalid API key",
			path:   "/ledgers",
			method: "GET",
			apiKey: "invalid-key",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Invalid API key",
		},
		{
			name:   "Expired API key",
			path:   "/ledgers",
			method: "GET",
			apiKey: expiredKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "API key is expired or revoked",
		},
		{
			name:   "Revoked API key",
			path:   "/ledgers",
			method: "GET",
			apiKey: revokedKey.Key,
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "API key is expired or revoked",
		},
		{
			name:   "Missing API key",
			path:   "/ledgers",
			method: "GET",
			apiKey: "",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Authentication required. Use X-Blnk-Key header",
		},
		{
			name:   "Comprehensive key for GET /ledgers",
			path:   "/ledgers",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /ledgers",
			path:   "/ledgers",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for DELETE /ledgers",
			path:   "/ledgers",
			method: "DELETE",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /accounts",
			path:   "/accounts",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /accounts",
			path:   "/accounts",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /transactions",
			path:   "/transactions",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /transactions",
			path:   "/transactions",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /refund-transaction",
			path:   "/refund-transaction/:id",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /identities",
			path:   "/identities",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /identities",
			path:   "/identities",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /balances",
			path:   "/balances",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /balance-monitors",
			path:   "/balance-monitors",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for POST /hooks",
			path:   "/hooks",
			method: "POST",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /api-keys",
			path:   "/api-keys",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /search",
			path:   "/search",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /multi-search",
			path:   "/multi-search",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /reconciliation",
			path:   "/reconciliation",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /metadata",
			path:   "/metadata",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for PATCH /metadata",
			path:   "/metadata",
			method: "PATCH",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Comprehensive key for GET /backup",
			path:   "/backup",
			method: "GET",
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure: true,
					},
				}
			},
			apiKey:       comprehensiveKey.Key,
			expectedCode: http.StatusOK,
		},
		{
			name:   "Near-miss master key is rejected",
			path:   "/ledgers",
			method: "GET",
			apiKey: "master-keX", // differs from SecretKey by one char
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure:    true,
						SecretKey: "master-key",
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Invalid API key",
		},
		{
			name:   "Empty configured secret key does not grant master access",
			path:   "/ledgers",
			method: "GET",
			apiKey: "anything", // no SecretKey configured -> must not be treated as master
			setupConfig: func() *config.Configuration {
				return &config.Configuration{
					Server: config.ServerConfig{
						Secure:    true,
						SecretKey: "",
					},
				}
			},
			expectedCode:  http.StatusUnauthorized,
			expectedError: "Invalid API key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blnk, err := setupBlnk()
			if err != nil {
				t.Fatalf("Failed to setup blnk: %v", err)
			}

			// Create a new router and middleware
			router := gin.New()
			authMiddleware := NewAuthMiddleware(blnk)

			// Store test configuration
			if tt.setupConfig != nil {
				config.ConfigStore.Store(tt.setupConfig())
			}

			// Add test route with middleware
			router.Any(tt.path, authMiddleware.Authenticate(), func(c *gin.Context) {
				c.Status(http.StatusOK)
			})

			// Create test request
			w := httptest.NewRecorder()
			req, _ := http.NewRequest(tt.method, tt.path, nil)
			if tt.apiKey != "" {
				req.Header.Set(KeyHeader, tt.apiKey)
			}

			// Serve the request
			router.ServeHTTP(w, req)

			// Assert response
			assert.Equal(t, tt.expectedCode, w.Code)
			if tt.expectedError != "" {
				assert.Contains(t, w.Body.String(), tt.expectedError)
			}
		})
	}
}

func TestGetResourceFromPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected Resource
	}{
		{
			name:     "Valid ledger path",
			path:     "/ledgers",
			expected: ResourceLedgers,
		},
		{
			name:     "Valid accounts path with ID",
			path:     "/accounts/123",
			expected: ResourceAccounts,
		},
		{
			name:     "Valid mocked account path",
			path:     "/mocked-account",
			expected: ResourceAccounts,
		},
		{
			name:     "Valid transactions path",
			path:     "/transactions",
			expected: ResourceTransactions,
		},
		{
			name:     "Valid identities path with ID",
			path:     "/identities/xyz",
			expected: ResourceIdentities,
		},
		{
			name:     "Valid balances path",
			path:     "/balances",
			expected: ResourceBalances,
		},
		{
			name:     "Valid balance-monitors path",
			path:     "/balance-monitors",
			expected: ResourceBalanceMonitors,
		},
		{
			name:     "Valid hooks path",
			path:     "/hooks",
			expected: ResourceHooks,
		},
		{
			name:     "Valid api-keys path",
			path:     "/api-keys",
			expected: ResourceAPIKeys,
		},
		{
			name:     "Valid search path",
			path:     "/search",
			expected: ResourceSearch,
		},
		{
			name:     "Valid reconciliation path",
			path:     "/reconciliation",
			expected: ResourceReconciliation,
		},
		{
			name:     "Valid metadata path",
			path:     "/metadata",
			expected: ResourceMetadata,
		},
		{
			name:     "Valid backup path",
			path:     "/backup",
			expected: ResourceBackup,
		},
		{
			name:     "Unknown resource",
			path:     "/unknown",
			expected: "",
		},
		{
			name:     "Empty path",
			path:     "",
			expected: "",
		},
		{
			name:     "Root path",
			path:     "/",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getResourceFromPath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractKey(t *testing.T) {
	tests := []struct {
		name     string
		header   string
		expected string
	}{
		{
			name:     "Valid key",
			header:   "test-key",
			expected: "test-key",
		},
		{
			name:     "Empty key",
			header:   "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("GET", "/", nil)
			if tt.header != "" {
				c.Request.Header.Set(KeyHeader, tt.header)
			}

			result := extractKey(c)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestMasterKeyRequest_ResolvesTheCredentialWhenNothingElseDecided covers the gate the
// privileged management surfaces read.
//
// Authenticate sets "isMasterKey" only on the path where it recognises the master key, and
// with SECURE MODE OFF it returns before comparing anything at all — authentication is
// disabled, so every route is open and nothing sets the value. Reading the value alone
// therefore refused the master key on that configuration, which is how the whole /events
// and /subscribers surface came to answer 403 AUTH_MASTER_KEY_REQUIRED on the shipped
// Compose default: the dead-letter triage and daily reconciliation runbooks are operated
// entirely through those endpoints, and neither could be run against a local stack.
//
// Both directions are asserted, because the value of the fix is that it changed exactly one
// of them. A caller presenting the configured master key must pass; a caller presenting
// nothing, the wrong key, or arriving at a deployment with no master key configured must
// still be refused — the fallback can only ever agree with the comparison Authenticate
// itself would have made.
func TestMasterKeyRequest_ResolvesTheCredentialWhenNothingElseDecided(t *testing.T) {
	const masterKey = "master-key-for-the-gate-test"

	// MockConfig runs the same validation the real loader does, so the required DSNs have to
	// be present or the configuration is refused and the previous test's config stays in the
	// store — which would make every assertion below read a value this test did not set.
	mockServer := func(t *testing.T, secure bool, secret string) {
		t.Helper()

		config.MockConfig(&config.Configuration{
			Redis:      config.RedisConfig{Dns: "localhost:6379"},
			DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
			Server:     config.ServerConfig{Secure: secure, SecretKey: secret},
		})

		conf, err := config.Fetch()
		require.NoError(t, err, "the mocked configuration must be readable")
		require.Equal(t, secret, conf.Server.SecretKey,
			"the mocked master key must have reached the config store, or this test asserts against "+
				"another test's configuration")
	}

	newRequest := func(t *testing.T, header string) *gin.Context {
		t.Helper()

		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/events/stats", nil)
		if header != "" {
			c.Request.Header.Set(KeyHeader, header)
		}

		return c
	}

	t.Run("the middleware's decision is authoritative wherever it made one", func(t *testing.T) {
		// Both arms, because the flag has to keep deciding in BOTH directions: an API-key caller
		// the middleware refused must stay refused even while holding a key that is not the
		// master one, and a master caller must not be re-checked.
		mockServer(t, true, masterKey)

		granted := newRequest(t, "")
		granted.Set("isMasterKey", true)
		assert.True(t, MasterKeyRequest(granted),
			"a request the middleware recognised must pass without presenting the header again")

		refused := newRequest(t, masterKey)
		refused.Set("isMasterKey", false)
		assert.False(t, MasterKeyRequest(refused),
			"an explicit false must not be overridden by the fallback; a middleware that has "+
				"already decided is the authority, and every handler test sets this value directly")
	})

	t.Run("with secure mode off the presented master key is recognised", func(t *testing.T) {
		// THE DEFECT. Nothing sets the context value on this path, so before the fallback this
		// returned false and the management surface refused its own documented credential.
		mockServer(t, false, masterKey)

		assert.True(t, MasterKeyRequest(newRequest(t, masterKey)),
			"the credential the runbooks name must work on the configuration the project ships")
	})

	t.Run("it stays fail-closed for everything else", func(t *testing.T) {
		mockServer(t, false, masterKey)

		assert.False(t, MasterKeyRequest(newRequest(t, "")),
			"no header is no credential: insecure mode opens the ordinary routes, it does not open "+
				"the privileged ones")
		assert.False(t, MasterKeyRequest(newRequest(t, "not-the-master-key")),
			"a wrong key must be refused, or the gate would be decoration")
		assert.False(t, MasterKeyRequest(newRequest(t, masterKey+"-suffixed")),
			"a key with the master key as a PREFIX must be refused; the comparison is on the whole "+
				"value and is constant time")

		t.Run("and when no master key is configured at all", func(t *testing.T) {
			// An unconfigured secret must never match. Comparing against "" would make every
			// caller — including one presenting an empty header — a master caller.
			mockServer(t, false, "")

			assert.False(t, MasterKeyRequest(newRequest(t, masterKey)))
			assert.False(t, MasterKeyRequest(newRequest(t, "")))
		})
	})

	t.Run("a nil context or a context with no request is not a master key request", func(t *testing.T) {
		// This runs inside request handlers, and a panic in an authorization gate would be a
		// denial of service on the surface it protects.
		mockServer(t, false, masterKey)

		assert.False(t, MasterKeyRequest(nil))

		bare, _ := gin.CreateTestContext(httptest.NewRecorder())
		assert.False(t, MasterKeyRequest(bare))
	})
}

func TestHasPermission(t *testing.T) {
	tests := []struct {
		name     string
		scopes   []string
		resource Resource
		method   string
		expected bool
	}{
		{
			name:     "Exact resource and action match",
			scopes:   []string{"ledgers:read"},
			resource: ResourceLedgers,
			method:   "GET",
			expected: true,
		},
		{
			name:     "Exact resource but wrong action",
			scopes:   []string{"ledgers:read"},
			resource: ResourceLedgers,
			method:   "POST",
			expected: false,
		},
		{
			name:     "Multiple explicit permissions - one matches",
			scopes:   []string{"ledgers:read", "accounts:write", "transactions:delete"},
			resource: ResourceLedgers,
			method:   "GET",
			expected: true,
		},
		{
			name:     "Multiple explicit permissions - none match",
			scopes:   []string{"ledgers:write", "accounts:write", "transactions:read"},
			resource: ResourceLedgers,
			method:   "GET",
			expected: false,
		},
		{
			name:     "Multiple explicit permissions - method match",
			scopes:   []string{"ledgers:write", "accounts:write", "transactions:read"},
			resource: ResourceLedgers,
			method:   "POST",
			expected: true,
		},
		{
			name:     "Unsupported HTTP method",
			scopes:   []string{"ledgers:read", "ledgers:write", "ledgers:delete"},
			resource: ResourceLedgers,
			method:   "CUSTOM",
			expected: false,
		},
		{
			name:     "Wildcard resource with matching action",
			scopes:   []string{"*:read"},
			resource: ResourceLedgers,
			method:   "GET",
			expected: true,
		},
		{
			name:     "Wildcard resource with all actions",
			scopes:   []string{"*:*"},
			resource: ResourceTransactions,
			method:   "DELETE",
			expected: true,
		},
		{
			name:     "Wildcard resource with wrong action",
			scopes:   []string{"*:read"},
			resource: ResourceLedgers,
			method:   "DELETE",
			expected: false,
		},
		{
			name:     "Resource with wildcard action",
			scopes:   []string{"ledgers:*"},
			resource: ResourceLedgers,
			method:   "DELETE",
			expected: true,
		},
		{
			name:     "HEAD method maps to read",
			scopes:   []string{"ledgers:read"},
			resource: ResourceLedgers,
			method:   "HEAD",
			expected: true,
		},
		{
			name:     "PUT method maps to write",
			scopes:   []string{"accounts:write"},
			resource: ResourceAccounts,
			method:   "PUT",
			expected: true,
		},
		{
			name:     "PATCH method maps to write",
			scopes:   []string{"accounts:write"},
			resource: ResourceAccounts,
			method:   "PATCH",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasPermission(tt.scopes, tt.resource, tt.method)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildScope(t *testing.T) {
	tests := []struct {
		name     string
		resource Resource
		action   Action
		expected string
	}{
		{
			name:     "ledgers read",
			resource: ResourceLedgers,
			action:   ActionRead,
			expected: "ledgers:read",
		},
		{
			name:     "accounts write",
			resource: ResourceAccounts,
			action:   ActionWrite,
			expected: "accounts:write",
		},
		{
			name:     "transactions delete",
			resource: ResourceTransactions,
			action:   ActionDelete,
			expected: "transactions:delete",
		},
		{
			name:     "wildcard resource with all actions",
			resource: ResourceAll,
			action:   ActionAll,
			expected: "*:*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildScope(tt.resource, tt.action)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseScope(t *testing.T) {
	tests := []struct {
		name             string
		scope            string
		expectedResource Resource
		expectedAction   Action
	}{
		{
			name:             "Valid scope ledgers:read",
			scope:            "ledgers:read",
			expectedResource: ResourceLedgers,
			expectedAction:   ActionRead,
		},
		{
			name:             "Valid scope accounts:write",
			scope:            "accounts:write",
			expectedResource: ResourceAccounts,
			expectedAction:   ActionWrite,
		},
		{
			name:             "Invalid scope - no colon",
			scope:            "ledgersread",
			expectedResource: "",
			expectedAction:   "",
		},
		{
			name:             "Invalid scope - too many parts",
			scope:            "ledgers:read:extra",
			expectedResource: "",
			expectedAction:   "",
		},
		{
			name:             "Empty scope",
			scope:            "",
			expectedResource: "",
			expectedAction:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resource, action := ParseScope(tt.scope)
			assert.Equal(t, tt.expectedResource, resource)
			assert.Equal(t, tt.expectedAction, action)
		})
	}
}

func TestScopeCovers(t *testing.T) {
	tests := []struct {
		name           string
		grantedScope   string
		requestedScope string
		expected       bool
	}{
		{
			name:           "Exact scope match",
			grantedScope:   "ledgers:read",
			requestedScope: "ledgers:read",
			expected:       true,
		},
		{
			name:           "Resource wildcard covers exact scope",
			grantedScope:   "*:write",
			requestedScope: "accounts:write",
			expected:       true,
		},
		{
			name:           "Action wildcard covers resource action",
			grantedScope:   "ledgers:*",
			requestedScope: "ledgers:delete",
			expected:       true,
		},
		{
			name:           "Exact resource scope does not cover wildcard request",
			grantedScope:   "ledgers:read",
			requestedScope: "ledgers:*",
			expected:       false,
		},
		{
			name:           "Different action is denied",
			grantedScope:   "ledgers:read",
			requestedScope: "ledgers:write",
			expected:       false,
		},
		{
			name:           "Invalid requested scope is denied",
			grantedScope:   "ledgers:*",
			requestedScope: "not-a-scope",
			expected:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ScopeCovers(tt.grantedScope, tt.requestedScope)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCanGrantScopes(t *testing.T) {
	tests := []struct {
		name            string
		callerScopes    []string
		requestedScopes []string
		expected        bool
	}{
		{
			name:            "All requested scopes are covered",
			callerScopes:    []string{"api-keys:read", "api-keys:write", "ledgers:*"},
			requestedScopes: []string{"api-keys:read", "ledgers:delete"},
			expected:        true,
		},
		{
			name:            "Wildcard caller can grant wildcard request",
			callerScopes:    []string{"*:*"},
			requestedScopes: []string{"accounts:delete"},
			expected:        true,
		},
		{
			name:            "Broader requested scope is denied",
			callerScopes:    []string{"api-keys:write"},
			requestedScopes: []string{"*:*"},
			expected:        false,
		},
		{
			name:            "Different resource is denied",
			callerScopes:    []string{"ledgers:*"},
			requestedScopes: []string{"accounts:read"},
			expected:        false,
		},
		{
			name:            "Invalid requested scope is denied",
			callerScopes:    []string{"ledgers:*"},
			requestedScopes: []string{"invalid"},
			expected:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CanGrantScopes(tt.callerScopes, tt.requestedScopes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRateLimitMiddleware(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("Rate limiting disabled when config is nil", func(t *testing.T) {
		conf := &config.Configuration{
			RateLimit: config.RateLimitConfig{
				RequestsPerSecond: nil,
				Burst:             nil,
			},
		}

		router := gin.New()
		router.Use(RateLimitMiddleware(conf))
		router.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/test", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("Rate limiting enabled", func(t *testing.T) {
		rps := 100.0
		burst := 10
		cleanup := 60

		conf := &config.Configuration{
			RateLimit: config.RateLimitConfig{
				RequestsPerSecond:  &rps,
				Burst:              &burst,
				CleanupIntervalSec: &cleanup,
			},
		}

		router := gin.New()
		router.Use(RateLimitMiddleware(conf))
		router.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		w := httptest.NewRecorder()
		req, _ := http.NewRequest("GET", "/test", nil)
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestInjectAPIKeyToMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("Non-POST request skips injection", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("GET", "/", nil)

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.NoError(t, err)
	})

	t.Run("Nil body skips injection", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/", nil)
		c.Request.Body = nil

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.NoError(t, err)
	})

	t.Run("Invalid JSON returns error", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		body := strings.NewReader("not valid json")
		c.Request = httptest.NewRequest("POST", "/", body)

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.Error(t, err)
	})

	t.Run("Valid JSON without meta_data creates it", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		body := strings.NewReader(`{"name": "test"}`)
		c.Request = httptest.NewRequest("POST", "/", body)

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.NoError(t, err)
	})

	t.Run("Valid JSON with existing meta_data updates it", func(t *testing.T) {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		body := strings.NewReader(`{"name": "test", "meta_data": {"existing": "value"}}`)
		c.Request = httptest.NewRequest("POST", "/", body)

		err := injectAPIKeyToMetadata(c, "api_key_123")
		assert.NoError(t, err)
	})
}
