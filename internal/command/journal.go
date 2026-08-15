package command

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	agentv1 "github.com/7K-Inari/inari-api/gen/go/inari/agent/v1"
)

// ConfigMapJournal is a Journal backed by a ConfigMap in the agent
// namespace: each command_id maps to its JSON-encoded ack. Mutation
// rights on this object are already within the agent's own footprint.
type ConfigMapJournal struct {
	Kube      kubernetes.Interface
	Namespace string
	Name      string

	mu sync.Mutex
}

// NewConfigMapJournal returns a Journal storing acks in a ConfigMap.
func NewConfigMapJournal(kube kubernetes.Interface, namespace, name string) *ConfigMapJournal {
	return &ConfigMapJournal{Kube: kube, Namespace: namespace, Name: name}
}

// Load implements Journal.
func (j *ConfigMapJournal) Load(ctx context.Context) (map[string]*agentv1.CommandAck, error) {
	cm, err := j.Kube.CoreV1().ConfigMaps(j.Namespace).Get(ctx, j.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return map[string]*agentv1.CommandAck{}, nil
	}
	if err != nil {
		return nil, err
	}
	out := map[string]*agentv1.CommandAck{}
	for id, raw := range cm.Data {
		var ack agentv1.CommandAck
		if err := json.Unmarshal([]byte(raw), &ack); err != nil {
			return nil, fmt.Errorf("journal: decode ack %s: %w", id, err)
		}
		out[id] = &ack
	}
	return out, nil
}

// Record implements Journal. Creates the ConfigMap on first use and
// retries update conflicts.
func (j *ConfigMapJournal) Record(ctx context.Context, ack *agentv1.CommandAck) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	raw, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	cms := j.Kube.CoreV1().ConfigMaps(j.Namespace)
	for attempt := 0; attempt < 3; attempt++ {
		cm, err := cms.Get(ctx, j.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			_, err = cms.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: j.Namespace, Name: j.Name},
				Data:       map[string]string{ack.CommandId: string(raw)},
			}, metav1.CreateOptions{})
			if err == nil || apierrors.IsAlreadyExists(err) {
				continue
			}
			return fmt.Errorf("journal: create: %w", err)
		}
		if err != nil {
			return fmt.Errorf("journal: get: %w", err)
		}
		if cm.Data == nil {
			cm.Data = map[string]string{}
		}
		cm.Data[ack.CommandId] = string(raw)
		if _, err := cms.Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			if apierrors.IsConflict(err) {
				continue
			}
			return fmt.Errorf("journal: update: %w", err)
		}
		return nil
	}
	return fmt.Errorf("journal: record %s: too many conflicts", ack.CommandId)
}

var _ Journal = (*ConfigMapJournal)(nil)
