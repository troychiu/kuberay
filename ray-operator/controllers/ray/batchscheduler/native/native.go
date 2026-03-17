package native

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktypes "k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	schedulerName string = "native"
)

type NativeGangScheduler struct {
	cli client.Client
}

type NativeGangSchedulerFactory struct{}

func GetPluginName() string {
	return schedulerName
}

func (n *NativeGangScheduler) Name() string {
	return schedulerName
}

func createWorkload(app *rayv1.RayCluster) *schedulingv1alpha2.Workload {
	return &schedulingv1alpha2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: app.Namespace,
			Name:      app.Name,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: app.APIVersion,
					Kind:       app.Kind,
					Name:       app.Name,
					UID:        app.UID,
				},
			},
		},
		Spec: schedulingv1alpha2.WorkloadSpec{
			ControllerRef: &schedulingv1alpha2.TypedLocalObjectReference{
				APIGroup: rayv1.GroupVersion.Group,
				Kind:     app.Kind,
				Name:     app.Name,
			},
			PodGroupTemplates: []schedulingv1alpha2.PodGroupTemplate{
				{
					Name: app.Name,
					SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
						Gang: &schedulingv1alpha2.GangSchedulingPolicy{
							MinCount: utils.CalculateDesiredReplicas(app) + 1, // +1 for the head pod
						},
					},
				},
			},
		},
	}
}

func createPodGroup(app *rayv1.RayCluster) *schedulingv1alpha2.PodGroup {
	return &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: app.Namespace,
			Name:      app.Name,
			OwnerReferences: []metav1.OwnerReference{
				{
					Name:       app.Name,
					UID:        app.UID,
					APIVersion: app.APIVersion,
					Kind:       app.Kind,
				},
			},
		},
		Spec: schedulingv1alpha2.PodGroupSpec{
			PodGroupTemplateRef: &schedulingv1alpha2.PodGroupTemplateReference{
				Workload: &schedulingv1alpha2.WorkloadPodGroupTemplateReference{
					WorkloadName:         app.Name,
					PodGroupTemplateName: app.Name,
				},
			},
			SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha2.GangSchedulingPolicy{
					MinCount: utils.CalculateDesiredReplicas(app) + 1, // +1 for the head pod
				},
			},
		},
	}
}

func (n *NativeGangScheduler) DoBatchSchedulingOnSubmission(ctx context.Context, object metav1.Object) error {
	app, ok := object.(*rayv1.RayCluster)
	if !ok {
		return fmt.Errorf("currently only RayCluster is supported, got %T", object)
	}
	if !n.isGangSchedulingEnabled(app) {
		return nil
	}
	// Create Workload
	workload := &schedulingv1alpha2.Workload{}
	if err := n.cli.Get(ctx, ktypes.NamespacedName{Namespace: app.Namespace, Name: app.Name}, workload); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		workload = createWorkload(app)
		if err := n.cli.Create(ctx, workload); err != nil {
			if errors.IsAlreadyExists(err) {
				return nil
			}
			return fmt.Errorf("failed to create Workload: %w", err)
		}
	}

	// Create PodGroup
	podGroup := &schedulingv1alpha2.PodGroup{}
	if err := n.cli.Get(ctx, ktypes.NamespacedName{Namespace: app.Namespace, Name: app.Name}, podGroup); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		podGroup = createPodGroup(app)
		if err := n.cli.Create(ctx, podGroup); err != nil {
			if errors.IsAlreadyExists(err) {
				return nil
			}
			return fmt.Errorf("failed to create PodGroup: %w", err)
		}
	}
	return nil
}

func (n *NativeGangScheduler) AddMetadataToChildResource(_ context.Context, parent metav1.Object, child metav1.Object, _ string) {
	app, ok := parent.(*rayv1.RayCluster)
	if !ok {
		return
	}
	if !n.isGangSchedulingEnabled(app) {
		return
	}

	pod, ok := child.(*corev1.Pod)
	if !ok {
		return
	}

	pod.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{
		PodGroupName: &app.Name,
	}
	return
}

func (n *NativeGangScheduler) isGangSchedulingEnabled(obj metav1.Object) bool {
	_, exist := obj.GetLabels()[utils.RayGangSchedulingEnabled]
	return exist
}

func (nf *NativeGangScheduler) CleanupOnCompletion(_ context.Context, _ metav1.Object) (bool, error) {
	return false, nil
}

func (nf *NativeGangSchedulerFactory) New(_ context.Context, _ *rest.Config, cli client.Client) (schedulerinterface.BatchScheduler, error) {
	if err := schedulingv1alpha2.AddToScheme(cli.Scheme()); err != nil {
		return nil, fmt.Errorf("failed to add schedulingv1alpha2 to scheme: %w", err)
	}
	return &NativeGangScheduler{
		cli: cli,
	}, nil
}

func (nf *NativeGangSchedulerFactory) AddToScheme(sche *runtime.Scheme) {
	utilruntime.Must(schedulingv1alpha2.AddToScheme(sche))
}

func (nf *NativeGangSchedulerFactory) ConfigureReconciler(b *builder.Builder) *builder.Builder {
	return b.Owns(&schedulingv1alpha2.PodGroup{})
}
