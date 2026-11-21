package verification

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/7K-Inari/inari-agent/internal/capability"
	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

var WorkflowGVR = schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "workflows"}

// Sender is the existing outbound stream client.
type Sender interface {
	Send(ctx context.Context, event *agentv1.Event) error
}

// Watcher observes only the optional labeled Workflow API.
type Watcher struct {
	Log       logr.Logger
	Dyn       dynamic.Interface
	Sender    Sender
	Discovery discovery.DiscoveryInterface
	Namespace string

	mu      sync.Mutex
	lastSum map[string]string
	runCtx  context.Context
}

func NewWatcher(log logr.Logger, dyn dynamic.Interface, sender Sender) *Watcher {
	return &Watcher{Log: log, Dyn: dyn, Sender: sender, lastSum: map[string]string{}}
}

// Reset clears content dedupe and explicitly relists current Workflow objects
// when Run is active. A disconnected period therefore cannot suppress the
// current snapshot on the next informer event.
func (w *Watcher) Reset() {
	w.mu.Lock()
	w.lastSum = map[string]string{}
	ctx := w.runCtx
	w.mu.Unlock()
	if ctx != nil && w.Dyn != nil {
		go w.resync(ctx)
	}
}

func (w *Watcher) Run(ctx context.Context) error {
	if w.Dyn == nil {
		<-ctx.Done()
		return nil
	}
	if w.Discovery != nil && !capability.ResourceServed(w.Discovery, WorkflowGVR) {
		w.Log.Info("verification watch skipped: Argo Workflows API is not installed")
		<-ctx.Done()
		return nil
	}
	w.mu.Lock()
	w.runCtx = ctx
	w.mu.Unlock()
	namespace := w.Namespace
	if namespace == "" {
		namespace = metav1.NamespaceAll
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(w.Dyn, 0, namespace, func(options *metav1.ListOptions) {
		options.LabelSelector = LabelVerification + "=true"
	})
	informer := factory.ForResource(WorkflowGVR).Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { w.emit(ctx, obj, false) },
		UpdateFunc: func(_, obj any) { w.emit(ctx, obj, false) },
		DeleteFunc: func(obj any) { w.emit(ctx, obj, true) },
	})
	if err != nil {
		return fmt.Errorf("verification: register Workflow handler: %w", err)
	}
	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		if ctx.Err() != nil {
			return nil
		}
		return errors.New("verification: Workflow cache sync aborted")
	}
	<-ctx.Done()
	return nil
}

func (w *Watcher) resync(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	namespace := w.Namespace
	if namespace == "" {
		namespace = metav1.NamespaceAll
	}
	list, err := w.Dyn.Resource(WorkflowGVR).Namespace(namespace).List(ctx, metav1.ListOptions{LabelSelector: LabelVerification + "=true"})
	if err != nil {
		w.Log.Error(err, "verification: resync Workflows")
		return
	}
	for i := range list.Items {
		w.emit(ctx, &list.Items[i], false)
	}
}

func (w *Watcher) emit(ctx context.Context, obj any, deleted bool) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	report, err := MapWorkflow(u, time.Now())
	if err != nil {
		w.Log.Info("verification: dropping invalid or unowned Workflow", "name", u.GetName(), "namespace", u.GetNamespace(), "reason", err.Error())
		return
	}
	report.Deleted = deleted
	checksumReport := proto.Clone(report).(*agentv1.VerificationReport)
	checksumReport.ObservedAt = nil // observation time is transport metadata, not content.
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(checksumReport)
	if err != nil {
		w.Log.Error(err, "verification: marshal report checksum")
		return
	}
	sum := sha256.Sum256(raw)
	checksum := hex.EncodeToString(sum[:])
	key := string(u.GetUID())
	w.mu.Lock()
	if w.lastSum[key] == checksum {
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()
	payload, err := anypb.New(report)
	if err != nil {
		w.Log.Error(err, "verification: pack report")
		return
	}
	event := &agentv1.Event{
		EventId: "verification-" + checksum[:24], ResourceId: report.RunId,
		Type:    agentv1.EventTypeString(agentv1.EventType_EVENT_TYPE_VERIFICATION_REPORT),
		Payload: payload, Time: timestamppb.Now(),
	}
	if w.Sender == nil {
		return
	}
	if err := w.Sender.Send(ctx, event); err != nil {
		w.Log.Error(err, "verification: send report", "run", report.RunId)
		return
	}
	w.mu.Lock()
	if deleted {
		delete(w.lastSum, key)
	} else {
		w.lastSum[key] = checksum
	}
	w.mu.Unlock()
}
