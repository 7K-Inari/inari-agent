package command

import (
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// Bridge command event types (Feature C, plan §6): the pinned agent
// contract has no RegisterArgoCDAppSet message and SyncPolicy cannot carry
// take-ownership syncOptions, so addon registrations flow as
// structpb.Struct bridge payloads until inari-api gains the messages
// (proto mod lands with the agent milestone). Payload shape, shared by
// both types:
//
//	{"commandId": "<bare logical id>", "name": "inari-addon[set]-<id>",
//	 "namespace": "argocd", "manifest": "<rendered YAML, upserted verbatim>"}
//
// The manifest is the committed application.yaml/applicationset.yaml —
// the authoritative desired state — so multi-source helm apps and
// TakeOwnership sync options survive the hop that RegisterArgoCDApp
// fields cannot express.
const (
	// RegisterArgoCDAppSetEventType registers a fleet ApplicationSet
	// (inari-addonset-<id>) from its rendered manifest.
	RegisterArgoCDAppSetEventType = "EVENT_TYPE_REGISTER_ARGOCD_APPSET"
	// RegisterArgoCDAppManifestEventType registers a single-cluster
	// addon Application (inari-addon-<id>) from its rendered manifest.
	RegisterArgoCDAppManifestEventType = "EVENT_TYPE_REGISTER_ARGOCD_APP_MANIFEST"
)

// IsBridgeCommandType reports whether an event type string is a
// server→agent bridge command (vs an agent→server bridge report like
// EVENT_TYPE_REPORT_ADDONS, which never flows inbound).
func IsBridgeCommandType(typeStr string) bool {
	switch typeStr {
	case RegisterArgoCDAppSetEventType, RegisterArgoCDAppManifestEventType:
		return true
	}
	return false
}

// RegisterBridge installs the handler for a structpb bridge command type,
// replacing any previous one.
func (d *Dispatcher) RegisterBridge(typeStr string, h KindHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bridgeHandlers[typeStr] = h
}

// DecodeBridgeManifest decodes the shared bridge payload shape.
func DecodeBridgeManifest(payload *anypb.Any) (commandID, name, namespace, manifest string, err error) {
	if payload == nil || !payload.MessageIs(&structpb.Struct{}) {
		return "", "", "", "", fmt.Errorf("command: bridge payload is not a struct")
	}
	var doc structpb.Struct
	if err := payload.UnmarshalTo(&doc); err != nil {
		return "", "", "", "", fmt.Errorf("command: decode bridge payload: %w", err)
	}
	f := doc.GetFields()
	get := func(key string) string {
		if v, ok := f[key]; ok {
			return v.GetStringValue()
		}
		return ""
	}
	commandID, name, namespace, manifest = get("commandId"), get("name"), get("namespace"), get("manifest")
	if commandID == "" {
		return "", "", "", "", fmt.Errorf("command: bridge payload missing commandId")
	}
	if name == "" {
		return "", "", "", "", fmt.Errorf("command: bridge payload missing name")
	}
	if manifest == "" {
		return "", "", "", "", fmt.Errorf("command: bridge payload missing manifest")
	}
	return commandID, name, namespace, manifest, nil
}

// bridgeCommandID extracts the command_id from a structpb bridge payload.
func bridgeCommandID(ev *agentv1.Event) (string, error) {
	if !ev.Payload.MessageIs(&structpb.Struct{}) {
		return "", fmt.Errorf("command: unknown event type %q", ev.Type)
	}
	var doc structpb.Struct
	if err := ev.Payload.UnmarshalTo(&doc); err != nil {
		return "", fmt.Errorf("command: decode bridge payload: %w", err)
	}
	return requireID(doc.GetFields()["commandId"].GetStringValue())
}
