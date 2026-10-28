package command

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

func bridgePayload(t *testing.T, fields map[string]interface{}) *anypb.Any {
	t.Helper()
	doc, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatalf("structpb: %v", err)
	}
	a, err := anypb.New(doc)
	if err != nil {
		t.Fatalf("anypb: %v", err)
	}
	return a
}

func TestDispatcherRoutesBridgeCommands(t *testing.T) {
	d := NewDispatcher()

	var gotID, gotName, gotManifest string
	d.RegisterBridge(RegisterArgoCDAppSetEventType, func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		id, name, _, manifest, err := DecodeBridgeManifest(ev.Payload)
		if err != nil {
			return agentv1.CommandResult_COMMAND_RESULT_FAILED, err.Error(), nil
		}
		gotID, gotName, gotManifest = id, name, manifest
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED, "ok", nil
	})

	ev := commandEvent(t, "appset", bridgePayload(t, map[string]interface{}{
		"commandId": "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1",
		"name":      "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1",
		"namespace": "argocd",
		"manifest":  "apiVersion: argoproj.io/v1alpha1\nkind: ApplicationSet\n",
	}), RegisterArgoCDAppSetEventType)

	ack, err := d.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_APPLIED {
		t.Fatalf("result = %v, want APPLIED", ack.Result)
	}
	if ack.CommandId != "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1" {
		t.Errorf("ack command id = %q, want payload commandId", ack.CommandId)
	}
	if gotID != "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1" || gotName != "inari-addonset-01j5z8k2n4xqj4tvdmx7a3b2c1" || gotManifest == "" {
		t.Errorf("handler got id=%q name=%q manifest=%q", gotID, gotName, gotManifest)
	}
}

func TestDispatcherBridgeCommandsAreIdempotent(t *testing.T) {
	d := NewDispatcher()
	ran := 0
	d.RegisterBridge(RegisterArgoCDAppManifestEventType, func(ctx context.Context, ev *agentv1.Event) (agentv1.CommandResult, string, error) {
		ran++
		return agentv1.CommandResult_COMMAND_RESULT_APPLIED, "ok", nil
	})
	ev := commandEvent(t, "appmanifest", bridgePayload(t, map[string]interface{}{
		"commandId": "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1",
		"name":      "inari-addon-01j5z8k2n4xqj4tvdmx7a3b2c1",
		"namespace": "argocd",
		"manifest":  "apiVersion: argoproj.io/v1alpha1\nkind: Application\n",
	}), RegisterArgoCDAppManifestEventType)

	if _, err := d.HandleEvent(context.Background(), ev); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := d.HandleEvent(context.Background(), ev); err != nil {
		t.Fatalf("second: %v", err)
	}
	if ran != 1 {
		t.Errorf("handler ran %d times, want 1 (idempotent replay)", ran)
	}
}

func TestDispatcherBridgeWithoutHandlerIsAcceptedNoop(t *testing.T) {
	d := NewDispatcher()
	ev := commandEvent(t, "nohandler", bridgePayload(t, map[string]interface{}{
		"commandId": "cmd-bridge",
	}), RegisterArgoCDAppSetEventType)
	ack, err := d.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	if ack.Result != agentv1.CommandResult_COMMAND_RESULT_ACCEPTED {
		t.Errorf("result = %v, want ACCEPTED no-op", ack.Result)
	}
}

func TestDecodeBridgeManifestRejectsBadPayloads(t *testing.T) {
	a, _ := anypb.New(&agentv1.ApplyBundle{CommandId: "cmd-1"})
	if _, _, _, _, err := DecodeBridgeManifest(a); err == nil {
		t.Error("non-struct payload: want error")
	}
	if _, _, _, _, err := DecodeBridgeManifest(bridgePayload(t, map[string]interface{}{
		"commandId": "cmd-2",
	})); err == nil {
		t.Error("missing name/manifest: want error")
	}
}

func TestIsBridgeCommandType(t *testing.T) {
	if !IsBridgeCommandType(RegisterArgoCDAppSetEventType) {
		t.Error("appset type should be a bridge command type")
	}
	if !IsBridgeCommandType(RegisterArgoCDAppManifestEventType) {
		t.Error("app-manifest type should be a bridge command type")
	}
	if IsBridgeCommandType(agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE)) {
		t.Error("proto command type is not a bridge type")
	}
	if IsBridgeCommandType("EVENT_TYPE_REPORT_ADDONS") {
		t.Error("report-addons is agent→server, not a command")
	}
}
