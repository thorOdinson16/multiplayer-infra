package provisioner

import (
	"context"
	"fmt"
	"log"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	statefulSetName := fmt.Sprintf("game-room-%s", matchID)
	serviceName := fmt.Sprintf("%s-headless", statefulSetName)
	labels := map[string]string{
		"app":     "game-room-server",
		"matchId": matchID,
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: p.namespace,
			Labels:    labels,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labels,
			Ports: []corev1.ServicePort{
				{Name: "http", Port: 8080, TargetPort: intstr.FromInt(8080)},
				{Name: "raft", Port: 7000, TargetPort: intstr.FromInt(7000)},
			},
		},
	}

	if _, err := p.client.CoreV1().Services(p.namespace).Create(context.Background(), svc, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create headless service: %w", err)
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      statefulSetName,
			Namespace: p.namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName:         serviceName,
			Replicas:            &replicas,
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
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
								{Name: "http", ContainerPort: 8080},
								{Name: "raft", ContainerPort: 7000},
							},
							Env: []corev1.EnvVar{
								{Name: "MATCH_ID", Value: matchID},
								{Name: "NODE_ID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
								{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
								{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
								{Name: "RAFT_BIND", Value: "$(POD_IP):7000"},
								{Name: "RAFT_SERVICE", Value: serviceName},
								{Name: "RAFT_NAMESPACE", Value: p.namespace},
								{Name: "RAFT_CLUSTER_SIZE", Value: "3"},
								{Name: "ETCD_ENDPOINTS", Value: "etcd.infra.svc.cluster.local:2379"},
								{Name: "KAFKA_BROKERS", Value: "kafka.infra.svc.cluster.local:9092"},
								{Name: "REDIS_ADDR", Value: "redis.infra.svc.cluster.local:6379"},
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

	log.Printf("Created game room StatefulSet: %s", statefulSetName)
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

	serviceName := fmt.Sprintf("%s-headless", name)
	if err := p.client.CoreV1().Services(p.namespace).Delete(context.Background(), serviceName, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("failed to delete headless service: %w", err)
	}

	log.Printf("Deleted game room StatefulSet: %s", name)
	return nil
}
