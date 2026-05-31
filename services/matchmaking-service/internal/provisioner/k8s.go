package provisioner

import (
	"context"
	"fmt"
	"log"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// K8sProvisioner creates game room StatefulSets via Kubernetes API (FR-MM-05)
type K8sProvisioner struct {
	client    kubernetes.Interface
	namespace string
}

// NewK8sProvisioner creates a new K8s provisioner
func NewK8sProvisioner(namespace string) (*K8sProvisioner, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %w", err)
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create K8s client: %w", err)
	}

	return &K8sProvisioner{
		client:    client,
		namespace: namespace,
	}, nil
}

// CreateGameRoom creates a new game room StatefulSet
func (p *K8sProvisioner) CreateGameRoom(matchID string, playerIDs []string) error {
	replicas := int32(3)
	labels := map[string]string{
		"app":     "game-room-server",
		"matchId": matchID,
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("game-room-%s", matchID),
			Namespace: p.namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: fmt.Sprintf("game-room-%s", matchID),
			Replicas:    &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "game-room-server",
							Image: "game-room-server:latest",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 8080},
							},
						},
					},
				},
			},
		},
	}

	_, err := p.client.AppsV1().StatefulSets(p.namespace).Create(
		context.Background(),
		sts,
		metav1.CreateOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to create StatefulSet: %w", err)
	}

	log.Printf("Created game room StatefulSet: game-room-%s", matchID)
	return nil
}

// DeleteGameRoom deletes a game room StatefulSet
func (p *K8sProvisioner) DeleteGameRoom(matchID string) error {
	name := fmt.Sprintf("game-room-%s", matchID)
	err := p.client.AppsV1().StatefulSets(p.namespace).Delete(
		context.Background(),
		name,
		metav1.DeleteOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to delete StatefulSet: %w", err)
	}

	log.Printf("Deleted game room StatefulSet: %s", name)
	return nil
}