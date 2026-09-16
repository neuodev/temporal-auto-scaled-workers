package computeprovider

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testCloudflareURL   = "https://waker.example.workers.dev/invoke"
	testCloudflareToken = "test-token"
)

func newCloudflareProvider() *cloudflareComputeProvider {
	return &cloudflareComputeProvider{
		credential: &cloudflareCredential{Type: cloudflareCredentialTypeBearer, Value: testCloudflareToken},
		client:     &http.Client{},
	}
}

func testCloudflareRequestContext() RequestContext {
	return RequestContext{
		NamespaceName:     "default",
		DeploymentName:    "orders",
		DeploymentBuildID: "build-123",
	}
}

func testCloudflareConfig() ComputeProviderConfig {
	return ComputeProviderConfig{configCloudflareURL: testCloudflareURL}
}

// stubCloudflareDo swaps the HTTP seam so tests never reach the network.
func stubCloudflareDo(t *testing.T, fn func(*http.Client, *http.Request) (*http.Response, error)) {
	orig := cloudflareDoFn
	cloudflareDoFn = fn
	t.Cleanup(func() { cloudflareDoFn = orig })
}

func cloudflareResponse(code int) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(""))}
}

func TestCloudflareLaunchStrategy(t *testing.T) {
	assert.Equal(t, LaunchStrategyInvoke, newCloudflareProvider().LaunchStrategy())
}

func TestCloudflareUpdateWorkerSetSizeUnsupported(t *testing.T) {
	err := newCloudflareProvider().UpdateWorkerSetSize(context.Background(), RequestContext{}, nil, 3)
	assert.ErrorIs(t, err, errors.ErrUnsupported)
}

func TestCloudflareInvokeWorkerSendsDeploymentIdentity(t *testing.T) {
	var gotReq *http.Request
	var gotBody []byte
	stubCloudflareDo(t, func(_ *http.Client, r *http.Request) (*http.Response, error) {
		gotReq = r
		gotBody, _ = io.ReadAll(r.Body)
		return cloudflareResponse(http.StatusAccepted), nil
	})

	cfg := testCloudflareConfig()
	cfg[configCloudflarePoolSize] = float64(4)

	err := newCloudflareProvider().InvokeWorker(context.Background(), testCloudflareRequestContext(), cfg)
	require.NoError(t, err)

	require.NotNil(t, gotReq)
	assert.Equal(t, http.MethodPost, gotReq.Method)
	assert.Equal(t, testCloudflareURL, gotReq.URL.String())
	assert.Equal(t, "Bearer "+testCloudflareToken, gotReq.Header.Get("Authorization"))
	assert.Equal(t, "application/json", gotReq.Header.Get("Content-Type"))

	var body cloudflareInvokeRequest
	require.NoError(t, json.Unmarshal(gotBody, &body))
	assert.Equal(t, "default", body.Namespace)
	assert.Equal(t, "orders", body.Deployment)
	assert.Equal(t, "build-123", body.BuildID)
	assert.Equal(t, int64(4), body.PoolSize)
	// A real invocation must not be mistaken for a preflight.
	assert.False(t, body.Validate)
}

func TestCloudflareInvokeWorkerFailureClassification(t *testing.T) {
	cases := []struct {
		name   string
		status int
		want   FailureClass
	}{
		{"accepted", http.StatusAccepted, FailureUnclassified},
		{"throttled", http.StatusTooManyRequests, FailureThrottled},
		{"not found", http.StatusNotFound, FailureNotFound},
		{"unauthorized", http.StatusUnauthorized, FailureAccessDenied},
		{"forbidden", http.StatusForbidden, FailureAccessDenied},
		{"server error", http.StatusInternalServerError, FailureUnavailable},
		{"bad gateway", http.StatusBadGateway, FailureUnavailable},
		{"bad request", http.StatusBadRequest, FailureRejected},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubCloudflareDo(t, func(*http.Client, *http.Request) (*http.Response, error) {
				return cloudflareResponse(tc.status), nil
			})

			err := newCloudflareProvider().InvokeWorker(context.Background(), testCloudflareRequestContext(), testCloudflareConfig())
			if tc.want == FailureUnclassified {
				assert.NoError(t, err)
				return
			}

			var provErr *ProviderError
			require.ErrorAs(t, err, &provErr)
			assert.Equal(t, tc.want, provErr.Class)
		})
	}
}

func TestCloudflareInvokeWorkerTransportErrorIsUnavailable(t *testing.T) {
	stubCloudflareDo(t, func(*http.Client, *http.Request) (*http.Response, error) {
		return nil, errors.New("dial tcp: connection refused")
	})

	err := newCloudflareProvider().InvokeWorker(context.Background(), testCloudflareRequestContext(), testCloudflareConfig())

	var provErr *ProviderError
	require.ErrorAs(t, err, &provErr)
	assert.Equal(t, FailureUnavailable, provErr.Class)
}

func TestCloudflareInvokeWorkerWithoutCredential(t *testing.T) {
	p := &cloudflareComputeProvider{client: &http.Client{}}
	err := p.InvokeWorker(context.Background(), testCloudflareRequestContext(), testCloudflareConfig())
	assert.ErrorContains(t, err, "no credential configured")
}

func TestCloudflareValidateConfigSetsValidateFlag(t *testing.T) {
	var gotBody []byte
	stubCloudflareDo(t, func(_ *http.Client, r *http.Request) (*http.Response, error) {
		gotBody, _ = io.ReadAll(r.Body)
		return cloudflareResponse(http.StatusNoContent), nil
	})

	err := newCloudflareProvider().ValidateConfig(context.Background(), testCloudflareRequestContext(), testCloudflareConfig())
	require.NoError(t, err)

	var body cloudflareInvokeRequest
	require.NoError(t, json.Unmarshal(gotBody, &body))
	// The endpoint relies on this to answer without starting a container.
	assert.True(t, body.Validate)
}

func TestCloudflareValidateConfigRejectsBadInput(t *testing.T) {
	cases := []struct {
		name     string
		provider *cloudflareComputeProvider
		cfg      ComputeProviderConfig
		contains string
	}{
		{
			name:     "no credential",
			provider: &cloudflareComputeProvider{client: &http.Client{}},
			cfg:      testCloudflareConfig(),
			contains: "credential_path",
		},
		{
			name:     "missing url",
			provider: newCloudflareProvider(),
			cfg:      ComputeProviderConfig{},
			contains: `requires "url"`,
		},
		{
			name:     "plaintext url",
			provider: newCloudflareProvider(),
			cfg:      ComputeProviderConfig{configCloudflareURL: "http://waker.example.com/invoke"},
			contains: "must use https",
		},
		{
			name:     "unparseable url",
			provider: newCloudflareProvider(),
			cfg:      ComputeProviderConfig{configCloudflareURL: "not-a-url"},
			contains: "not a valid URL",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// No seam stub: these must all fail before any request is attempted.
			stubCloudflareDo(t, func(*http.Client, *http.Request) (*http.Response, error) {
				t.Fatal("expected no HTTP request")
				return nil, nil
			})
			err := tc.provider.ValidateConfig(context.Background(), testCloudflareRequestContext(), tc.cfg)
			assert.ErrorContains(t, err, tc.contains)
		})
	}
}

func TestCloudflareValidateConfigSurfacesEndpointFailure(t *testing.T) {
	stubCloudflareDo(t, func(*http.Client, *http.Request) (*http.Response, error) {
		return cloudflareResponse(http.StatusForbidden), nil
	})

	err := newCloudflareProvider().ValidateConfig(context.Background(), testCloudflareRequestContext(), testCloudflareConfig())
	assert.ErrorContains(t, err, "cannot reach the wake endpoint")
}

func TestLoadCloudflareCredential(t *testing.T) {
	cases := []struct {
		name     string
		contents string
		wantErr  string
	}{
		{
			name:     "valid bearer",
			contents: `{"type":"bearer","value":"s3cret"}`,
		},
		{
			name:     "malformed json",
			contents: `{"type":"bearer"`,
			wantErr:  "not valid JSON",
		},
		{
			name:     "unsupported type",
			contents: `{"type":"mtls","value":"s3cret"}`,
			wantErr:  "is not supported",
		},
		{
			name:     "empty value",
			contents: `{"type":"bearer","value":""}`,
			wantErr:  "empty value",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "credential.json")
			require.NoError(t, os.WriteFile(path, []byte(tc.contents), 0o600))

			credential, err := loadCloudflareCredential(path)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
				assert.Nil(t, credential)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, cloudflareCredentialTypeBearer, credential.Type)
			assert.Equal(t, "s3cret", credential.Value)
		})
	}
}

func TestLoadCloudflareCredentialMissingFile(t *testing.T) {
	_, err := loadCloudflareCredential(filepath.Join(t.TempDir(), "absent.json"))
	assert.ErrorContains(t, err, "cannot read cloudflare credential file")
}

func TestCloudflarePoolSize(t *testing.T) {
	cases := []struct {
		name string
		cfg  ComputeProviderConfig
		want int64
	}{
		{"absent", ComputeProviderConfig{}, cloudflareDefaultPoolSize},
		{"valid", ComputeProviderConfig{configCloudflarePoolSize: float64(8)}, 8},
		{"zero falls back", ComputeProviderConfig{configCloudflarePoolSize: float64(0)}, cloudflareDefaultPoolSize},
		{"negative falls back", ComputeProviderConfig{configCloudflarePoolSize: float64(-3)}, cloudflareDefaultPoolSize},
		{"above max falls back", ComputeProviderConfig{configCloudflarePoolSize: float64(cloudflareMaxPoolSize + 1)}, cloudflareDefaultPoolSize},
		// Spec config arrives via a JSON payload, so a non-float type is not expected.
		{"wrong type falls back", ComputeProviderConfig{configCloudflarePoolSize: "8"}, cloudflareDefaultPoolSize},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, cloudflarePoolSize(tc.cfg))
		})
	}
}
