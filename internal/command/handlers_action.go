package command

import (
	"context"
	"errors"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"

	"github.com/7K-Inari/inari-agent/internal/argocd"
)

// InvokeActionDeps wires the invoke-action handler: a narrow ArgoCD API
// client plus cluster access for the ownership check.
type InvokeActionDeps struct {
	API *argocd.APIClient
	Dyn dynamic.Interface
	// Namespace is the ArgoCD install namespace. Ignored when
	// ResolveNamespace is set.
	Namespace string
	// ResolveNamespace, when set, resolves the live ArgoCD namespace per
	// invocation (e.g. Lifecycle.EnsureReady) so BYO-adopted installs in
	// non-default namespaces are found.
	ResolveNamespace func(ctx context.Context) (string, error)
	// Redeemer exchanges InvokeAction.user_credential_ref for a per-command
	// user ArgoCD bearer. Nil means per-user credentials are not configured:
	// any command carrying a ref fails closed.
	Redeemer UserCredentialRedeemer
}

// allowedActions is the M2 allow-list for tunneled imperative ops.
var allowedActions = map[string]bool{"sync": true, "refresh": true, "rollback": true}

// InvokeActionHandler tunnels imperative ops (sync/refresh/rollback)
// through the tenant-local ArgoCD API, scoped to Inari-managed
// Applications (plan §5.3). The dispatcher's fail-closed gate already
// blocks execution while the stream is disconnected.
func InvokeActionHandler(deps InvokeActionDeps) KindHandler {
	return func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		var m agentv1.InvokeAction
		if err := ev.Payload.UnmarshalTo(&m); err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, "", fmt.Errorf("decode: %w", err)
		}
		action := strings.ToLower(m.Action)
		if !allowedActions[action] {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED,
				fmt.Sprintf("action %q not in allow-list (sync|refresh|rollback)", m.Action), nil
		}
		if m.Resource == nil || m.Resource.Name == "" {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, "missing target resource", nil
		}
		if m.Resource.Kind != "" && !strings.EqualFold(m.Resource.Kind, "Application") {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED,
				fmt.Sprintf("invoke-action targets Applications only, got kind %q", m.Resource.Kind), nil
		}

		// Ownership scoping: refuse actions on resources Inari does not
		// manage (brownfield observe-only, §12.1/3).
		ns := deps.Namespace
		if deps.ResolveNamespace != nil {
			resolved, err := deps.ResolveNamespace(ctx)
			if err != nil {
				return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
			}
			ns = resolved
		}
		app, err := deps.Dyn.Resource(argocd.ApplicationGVR).Namespace(ns).
			Get(ctx, m.Resource.Name, metav1.GetOptions{})
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED,
				fmt.Sprintf("resolve application %s: %v", m.Resource.Name, err), nil
		}
		if app.GetLabels()[argocd.ManagedByLabel] != argocd.ManagedByValue {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED,
				fmt.Sprintf("application %s is not Inari-managed; refusing out-of-scope action", m.Resource.Name), nil
		}

		if t := m.Timeout; t != nil && t.AsDuration() > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, t.AsDuration())
			defer cancel()
		}

		// Per-user credential pass-through: redeem the opaque ref after the
		// ownership check so one-shot refs are never consumed by out-of-scope
		// rejects. The bearer lives only in this command's context; its TTL
		// is bounded by the command timeout above. Terminal redemption
		// failures are FAILED results (no redelivery); transient ones surface
		// as handler errors for the dispatcher's normal retry path.
		if m.UserCredentialRef != "" {
			if deps.Redeemer == nil {
				return agentv1.CommandResult_COMMAND_RESULT_FAILED,
					"per-user credentials not configured on this agent; refusing user-scoped action", nil
			}
			bearer, _, rerr := deps.Redeemer.Redeem(ctx, m.UserCredentialRef)
			if rerr != nil {
				var re *RedeemError
				if errors.As(rerr, &re) && re.Terminal() {
					return agentv1.CommandResult_COMMAND_RESULT_FAILED, re.Error(), nil
				}
				return agentv1.CommandResult_COMMAND_RESULT_UNSPECIFIED, "",
					fmt.Errorf("redeem user credential: %w", rerr)
			}
			if bearer == "" {
				// An empty bearer would silently fall back to the static
				// break-glass token; fail closed instead.
				return agentv1.CommandResult_COMMAND_RESULT_FAILED,
					"user credential redemption returned no token", nil
			}
			ctx = argocd.WithBearer(ctx, bearer)
		}

		params := map[string]any{}
		if m.Parameters != nil {
			params = m.Parameters.AsMap()
		}
		switch action {
		case "sync":
			err = deps.API.Sync(ctx, m.Resource.Name, params)
		case "refresh":
			hard, _ := params["hard"].(bool)
			err = deps.API.Refresh(ctx, m.Resource.Name, hard)
		case "rollback":
			id, ok := toInt64(params["id"])
			if !ok {
				return agentv1.CommandResult_COMMAND_RESULT_FAILED, "rollback needs numeric parameter id", nil
			}
			dryRun, _ := params["dryRun"].(bool)
			err = deps.API.Rollback(ctx, m.Resource.Name, id, dryRun)
		}
		if err != nil {
			// API failures are transient from the control plane's
			// perspective; surface as error so redelivery can retry.
			return agentv1.CommandResult_COMMAND_RESULT_UNSPECIFIED, "",
				fmt.Errorf("invoke %s on %s: %w", action, m.Resource.Name, err)
		}
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED,
			fmt.Sprintf("%s invoked on application %s", action, m.Resource.Name), nil
	}
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case string:
		var out int64
		_, err := fmt.Sscan(n, &out)
		return out, err == nil
	}
	return 0, false
}
