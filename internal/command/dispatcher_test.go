package command

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

func commandEvent(t *testing.T, id string, payload *anypb.Any, typeStr string) *agentv1.Event {
	t.Helper()
	return &agentv1.Event{EventId: "evt-" + id, Type: typeStr, Payload: payload}
}

func TestDispatcherAcksAllKnownCommandKinds(t *testing.T) {
	d := NewDispatcher()

	kinds := []struct {
		typeStr string
		payload func() *anypb.Any
	}{
		{agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE), func() *anypb.Any {
			a, _ := anypb.New(&agentv1.ApplyBundle{CommandId: "cmd-1"})
			return a
		}},
		{agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_REGISTER_ARGOCD_APP), func() *anypb.Any {
			a, _ := anypb.New(&agentv1.RegisterArgoCDApp{CommandId: "cmd-2"})
			return a
		}},
		{agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_INVOKE_ACTION), func() *anypb.Any {
			a, _ := anypb.New(&agentv1.InvokeAction{CommandId: "cmd-3"})
			return a
		}},
		{agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_RENDER_RGD_INSTANCE), func() *anypb.Any {
			a, _ := anypb.New(&agentv1.RenderRgdInstance{CommandId: "cmd-4"})
			return a
		}},
	}
	for _, k := range kinds {
		ack, err := d.HandleEvent(context.Background(), commandEvent(t, "x", k.payload(), k.typeStr))
		if err != nil {
			t.Fatalf("HandleEvent(%s): %v", k.typeStr, err)
		}
		if ack.Result != agentv1.CommandResult_COMMAND_RESULT_ACCEPTED {
			t.Errorf("%s result = %v, want ACCEPTED (no-op until M2)", k.typeStr, ack.Result)
		}
	}
}

func TestDispatcherIsIdempotent(t *testing.T) {
	d := NewDispatcher()
	a, _ := anypb.New(&agentv1.ApplyBundle{CommandId: "cmd-dup"})
	ev := commandEvent(t, "dup", a, agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_APPLY_BUNDLE))

	first, err := d.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := d.HandleEvent(context.Background(), ev)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if first.CommandId != second.CommandId || first.Result != second.Result || first.Message != second.Message {
		t.Errorf("duplicate delivery must return the recorded result: %+v vs %+v", first, second)
	}
	if d.HandledCount() != 1 {
		t.Errorf("handler executed %d times, want 1", d.HandledCount())
	}
}

func TestDispatcherNacksUnknownType(t *testing.T) {
	d := NewDispatcher()
	_, err := d.HandleEvent(context.Background(), &agentv1.Event{EventId: "e", Type: "inari.agent.unknown.v1"})
	if err == nil {
		t.Fatal("unknown event type must be rejected")
	}
}

func TestDispatcherFailsClosedWhenDisconnected(t *testing.T) {
	d := NewDispatcher()
	d.SetConnected(false)
	a, _ := anypb.New(&agentv1.InvokeAction{CommandId: "cmd-5"})
	_, err := d.HandleEvent(context.Background(), commandEvent(t, "x", a, agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_INVOKE_ACTION)))
	if err == nil {
		t.Fatal("commands must fail closed while disconnected (plan §5.3)")
	}
}
