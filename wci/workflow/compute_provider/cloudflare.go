package computeprovider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"go.temporal.io/auto-scaled-workers/wci/client"
	"go.temporal.io/auto-scaled-workers/wci/workflow/iface"
	"go.temporal.io/server/common/dynamicconfig"
)

const (
	configCloudflareURL      = "url"
	configCloudflarePoolSize = "pool_size"

	// The wake endpoint acks and boots the container asynchronously, so this bounds
	// the ack only, not container start-up.
	cloudflareRequestTimeout = 5 * time.Second

	cloudflareDefaultPoolSize = 1
	cloudflareMaxPoolSize     = 64

	cloudflareCredentialTypeBearer = "bearer"
)

type cloudflareComputeProvider struct {
	credential *cloudflareCredential
	client     *http.Client
}

// cloudflareCredential is the on-disk JSON credential. Only bearer is supported today;
// the type field exists so mTLS or Cloudflare Access can be added without a format change.
type cloudflareCredential struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type cloudflareInvokeRequest struct {
	Namespace  string `json:"namespace"`
	Deployment string `json:"deployment"`
	BuildID    string `json:"build_id"`
	PoolSize   int64  `json:"pool_size"`
	// Validate asks the endpoint to check routing and credentials without starting a container.
	Validate bool `json:"validate,omitempty"`
}

func init() {
	RegisterComputeProvider(iface.ComputeProviderTypeCloudflareContainer, NewCloudflareComputeProvider)
}

func NewCloudflareComputeProvider(_ context.Context, dc *dynamicconfig.Collection) (ComputeProvider, error) {
	var credential *cloudflareCredential
	if dc != nil {
		if path := client.WorkerControllerCloudflareCredentialPath.Get(dc)(); path != nil {
			loaded, err := loadCloudflareCredential(*path)
			if err != nil {
				return nil, err
			}
			credential = loaded
		}
	}
	return &cloudflareComputeProvider{
		credential: credential,
		client:     &http.Client{Timeout: cloudflareRequestTimeout},
	}, nil
}

func loadCloudflareCredential(path string) (*cloudflareCredential, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read cloudflare credential file: %w", err)
	}

	var credential cloudflareCredential
	if err := json.Unmarshal(raw, &credential); err != nil {
		return nil, fmt.Errorf("cloudflare credential file %q is not valid JSON: %w", path, err)
	}
	if credential.Type != cloudflareCredentialTypeBearer {
		return nil, fmt.Errorf("cloudflare credential type %q is not supported, expected %q", credential.Type, cloudflareCredentialTypeBearer)
	}
	if credential.Value == "" {
		return nil, fmt.Errorf("cloudflare credential file %q has an empty value", path)
	}
	return &credential, nil
}

func (p *cloudflareComputeProvider) LaunchStrategy() LaunchStrategy {
	return LaunchStrategyInvoke
}

func (p *cloudflareComputeProvider) ValidateConfig(ctx context.Context, rc RequestContext, cfg ComputeProviderConfig) error {
	if p.credential == nil {
		return fmt.Errorf("cloudflare compute provider requires workercontroller.compute_providers.cloudflare.credential_path to be configured")
	}
	if _, err := cloudflareURL(cfg); err != nil {
		return err
	}

	// Mirrors the Lambda provider's GetFunction preflight: prove the endpoint exists and
	// accepts our credentials now, rather than at the first scale-up. The endpoint must
	// not start a container when validate is set.
	if err := p.post(ctx, rc, cfg, true); err != nil {
		return fmt.Errorf("cannot reach the wake endpoint: %w", err)
	}
	return nil
}

func (p *cloudflareComputeProvider) InvokeWorker(ctx context.Context, rc RequestContext, cfg ComputeProviderConfig) error {
	err := p.post(ctx, rc, cfg, false)
	return NewProviderError(classifyCloudflareFailure(err), err)
}

func (p *cloudflareComputeProvider) UpdateWorkerSetSize(_ context.Context, _ RequestContext, _ ComputeProviderConfig, _ int32) error {
	return errors.ErrUnsupported
}

func (p *cloudflareComputeProvider) post(ctx context.Context, rc RequestContext, cfg ComputeProviderConfig, validate bool) error {
	if p.credential == nil {
		return fmt.Errorf("cloudflare compute provider has no credential configured")
	}
	endpoint, err := cloudflareURL(cfg)
	if err != nil {
		return err
	}
	body, err := json.Marshal(cloudflareInvokeRequest{
		Namespace:  rc.NamespaceName,
		Deployment: rc.DeploymentName,
		BuildID:    rc.DeploymentBuildID,
		PoolSize:   cloudflarePoolSize(cfg),
		Validate:   validate,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.credential.Value)

	resp, err := cloudflareDoFn(p.client, req)
	if err != nil {
		return fmt.Errorf("failed to call cloudflare wake endpoint: %w", err)
	}
	defer resp.Body.Close()
	// Drain so the connection is reused across a burst of invocations.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 300 {
		return &cloudflareStatusError{code: resp.StatusCode}
	}
	return nil
}

// cloudflareDoFn is a package-level var so tests can swap it without reaching the network.
var cloudflareDoFn = func(c *http.Client, r *http.Request) (*http.Response, error) { return c.Do(r) }

type cloudflareStatusError struct{ code int }

func (e *cloudflareStatusError) Error() string {
	return fmt.Sprintf("cloudflare wake endpoint returned HTTP %d", e.code)
}

func classifyCloudflareFailure(err error) FailureClass {
	if err == nil {
		return FailureUnclassified
	}

	var statusErr *cloudflareStatusError
	if !errors.As(err, &statusErr) {
		// Transport, DNS or timeout: the endpoint may simply be unreachable right now.
		return FailureUnavailable
	}

	switch {
	case statusErr.code == http.StatusTooManyRequests:
		return FailureThrottled
	case statusErr.code == http.StatusNotFound:
		return FailureNotFound
	case statusErr.code == http.StatusUnauthorized, statusErr.code == http.StatusForbidden:
		return FailureAccessDenied
	case statusErr.code >= 500:
		return FailureUnavailable
	default:
		return FailureRejected
	}
}

func cloudflareURL(cfg ComputeProviderConfig) (string, error) {
	raw, _ := cfg[configCloudflareURL].(string)
	if raw == "" {
		return "", fmt.Errorf("cloudflare compute provider requires %q to be configured", configCloudflareURL)
	}

	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("%q is not a valid URL", configCloudflareURL)
	}
	// The bearer token is the only thing guarding container start-up, so refuse to send it in clear.
	if u.Scheme != "https" {
		return "", fmt.Errorf("%q must use https", configCloudflareURL)
	}
	return raw, nil
}

// Spec config round-trips through a JSON payload, so a number arrives as float64.
func cloudflarePoolSize(cfg ComputeProviderConfig) int64 {
	n, ok := cfg[configCloudflarePoolSize].(float64)
	if !ok || n < 1 || n > cloudflareMaxPoolSize {
		return cloudflareDefaultPoolSize
	}
	return int64(n)
}
