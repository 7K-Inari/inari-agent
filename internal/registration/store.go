package registration

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// KubeSecretReader resolves the ESO-delivered OIDC client secret from the
// tenant cluster. ESO sync is asynchronous, so reads poll until the Secret
// materialises or the timeout elapses.
type KubeSecretReader struct {
	Client kubernetes.Interface

	// PollInterval between Secret lookups (default 2s).
	PollInterval time.Duration
	// PollTimeout for the overall wait (default 2m).
	PollTimeout time.Duration
}

// ReadSecret implements SecretReader.
func (r *KubeSecretReader) ReadSecret(ctx context.Context, ref SecretReference) (string, error) {
	if ref.Name == "" || ref.Namespace == "" || ref.Key == "" {
		return "", fmt.Errorf("registration: incomplete secret reference %+v", ref)
	}
	interval := r.PollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	timeout := r.PollTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}

	var value string
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		secret, err := r.Client.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		raw, ok := secret.Data[ref.Key]
		if !ok || len(raw) == 0 {
			return false, fmt.Errorf("registration: secret %s/%s has no key %q", ref.Namespace, ref.Name, ref.Key)
		}
		value = string(raw)
		return true, nil
	})
	if err != nil {
		return "", fmt.Errorf("registration: wait for ESO secret %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	return value, nil
}
