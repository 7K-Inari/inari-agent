package command

import (
	"context"
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
	// Namespace is the ArgoCD install namespace.
	Namespace string
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
		app, err := deps.Dyn.Resource(argocd.ApplicationGVR).Namespace(deps.Namespace).
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
