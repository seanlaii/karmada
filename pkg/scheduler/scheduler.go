/*
Copyright 2021 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	policyv1alpha1 "github.com/karmada-io/karmada/pkg/apis/policy/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	estimatorclient "github.com/karmada-io/karmada/pkg/estimator/client"
	"github.com/karmada-io/karmada/pkg/events"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	informerfactory "github.com/karmada-io/karmada/pkg/generated/informers/externalversions"
	clusterlister "github.com/karmada-io/karmada/pkg/generated/listers/cluster/v1alpha1"
	federatedlister "github.com/karmada-io/karmada/pkg/generated/listers/work/v1alpha1"
	worklister "github.com/karmada-io/karmada/pkg/generated/listers/work/v1alpha2"
	schedulercache "github.com/karmada-io/karmada/pkg/scheduler/cache"
	"github.com/karmada-io/karmada/pkg/scheduler/core"
	"github.com/karmada-io/karmada/pkg/scheduler/framework"
	frameworkplugins "github.com/karmada-io/karmada/pkg/scheduler/framework/plugins"
	"github.com/karmada-io/karmada/pkg/scheduler/framework/runtime"
	"github.com/karmada-io/karmada/pkg/scheduler/metrics"
	"github.com/karmada-io/karmada/pkg/scheduler/priorityqueue"
	"github.com/karmada-io/karmada/pkg/sharedcli/ratelimiterflag"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/grpcconnection"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/lifted/scheduler/framework/parallelize"
	utilmetrics "github.com/karmada-io/karmada/pkg/util/metrics"
)

// ScheduleType defines the schedule type of a binding object should be performed.
type ScheduleType string

const (
	// ReconcileSchedule means the binding object associated policy has been changed.
	ReconcileSchedule ScheduleType = "ReconcileSchedule"

	// ScaleSchedule means the replicas of binding object has been changed.
	ScaleSchedule ScheduleType = "ScaleSchedule"
)

const (
	// DefaultScheduler defines the name of default scheduler.
	DefaultScheduler = "default-scheduler"
)

const (
	// successfulSchedulingMessage defines the successful binding event message.
	successfulSchedulingMessage = "Binding has been scheduled successfully."
)

// Scheduler is the scheduler schema, which is used to schedule a specific resource to specific clusters
type Scheduler struct {
	DynamicClient                dynamic.Interface
	KarmadaClient                karmadaclientset.Interface
	KubeClient                   kubernetes.Interface
	bindingLister                worklister.ResourceBindingLister
	federatedPriorityClassLister federatedlister.FederatedPriorityClassLister
	clusterBindingLister         worklister.ClusterResourceBindingLister
	clusterLister                clusterlister.ClusterLister
	informerFactory              informerfactory.SharedInformerFactory
	parallelizer                 parallelize.Parallelizer

	// clusterReconcileWorker reconciles cluster changes to trigger corresponding
	// ResourceBinding/ClusterResourceBinding rescheduling.
	clusterReconcileWorker util.AsyncWorker
	// TODO: implement a priority scheduling queue
	queue workqueue.RateLimitingInterface

	Algorithm      core.ScheduleAlgorithm
	schedulerCache schedulercache.Cache

	eventRecorder record.EventRecorder

	enablePriorityQueue                 bool
	enableSchedulerEstimator            bool
	disableSchedulerEstimatorInPullMode bool
	schedulerEstimatorCache             *estimatorclient.SchedulerEstimatorCache
	schedulerEstimatorServiceNamespace  string
	schedulerEstimatorServicePrefix     string
	schedulerEstimatorWorker            util.AsyncWorker
	schedulerEstimatorClientConfig      *grpcconnection.ClientConfig
	schedulerName                       string

	enableEmptyWorkloadPropagation bool
}

type schedulerOptions struct {
	// enablePriorityQueue represents whether the priority queue should be enabled.
	enablePriorityQueue bool
	// enableSchedulerEstimator represents whether the accurate scheduler estimator should be enabled.
	enableSchedulerEstimator bool
	// disableSchedulerEstimatorInPullMode represents whether to disable the scheduler estimator in pull mode.
	disableSchedulerEstimatorInPullMode bool
	// schedulerEstimatorTimeout specifies the timeout period of calling the accurate scheduler estimator service.
	schedulerEstimatorTimeout metav1.Duration
	// schedulerEstimatorServiceNamespace specifies the namespace to be used for discovering scheduler estimator services.
	schedulerEstimatorServiceNamespace string
	// SchedulerEstimatorServicePrefix presents the prefix of the accurate scheduler estimator service name.
	schedulerEstimatorServicePrefix string
	// schedulerName is the name of the scheduler. Default is "default-scheduler".
	schedulerName string
	//enableEmptyWorkloadPropagation represents whether allow workload with replicas 0 propagated to member clusters should be enabled
	enableEmptyWorkloadPropagation bool
	// outOfTreeRegistry represents the registry of out-of-tree plugins
	outOfTreeRegistry runtime.Registry
	// plugins is the list of plugins to enable or disable
	plugins []string
	// contains the options for rate limiter.
	RateLimiterOptions ratelimiterflag.Options
	// schedulerEstimatorClientConfig contains the configuration of GRPC.
	schedulerEstimatorClientConfig *grpcconnection.ClientConfig
	// Parallelism defines the amount of parallelism in algorithms for preemption. Must be greater than 0. Defaults to 16.
	parallelism int
}

// Option configures a Scheduler
type Option func(*schedulerOptions)

// WithEnablePriorityQueue sets the enablePriorityQueue for scheduler
func WithEnablePriorityQueue(enablePriorityQueue bool) Option {
	return func(o *schedulerOptions) {
		o.enablePriorityQueue = enablePriorityQueue
	}
}

// WithSchedulerEstimatorServiceNamespace sets the schedulerEstimatorServiceNamespace for the scheduler
func WithSchedulerEstimatorServiceNamespace(schedulerEstimatorServiceNamespace string) Option {
	return func(o *schedulerOptions) {
		o.schedulerEstimatorServiceNamespace = schedulerEstimatorServiceNamespace
	}
}

// WithEnableSchedulerEstimator sets the enableSchedulerEstimator for scheduler
func WithParallelism(parallelism int) Option {
	return func(o *schedulerOptions) {
		o.parallelism = parallelism
	}
}

// WithEnableSchedulerEstimator sets the enableSchedulerEstimator for scheduler
func WithEnableSchedulerEstimator(enableSchedulerEstimator bool) Option {
	return func(o *schedulerOptions) {
		o.enableSchedulerEstimator = enableSchedulerEstimator
	}
}

// WithSchedulerEstimatorConnection sets the grpc config for scheduler
func WithSchedulerEstimatorConnection(port int, certFile, keyFile, trustedCAFile string, insecureSkipVerify bool) Option {
	return func(o *schedulerOptions) {
		o.schedulerEstimatorClientConfig = &grpcconnection.ClientConfig{
			CertFile:                 certFile,
			KeyFile:                  keyFile,
			ServerAuthCAFile:         trustedCAFile,
			InsecureSkipServerVerify: insecureSkipVerify,
			TargetPort:               port,
		}
	}
}

// WithDisableSchedulerEstimatorInPullMode sets the disableSchedulerEstimatorInPullMode for scheduler
func WithDisableSchedulerEstimatorInPullMode(disableSchedulerEstimatorInPullMode bool) Option {
	return func(o *schedulerOptions) {
		o.disableSchedulerEstimatorInPullMode = disableSchedulerEstimatorInPullMode
	}
}

// WithSchedulerEstimatorTimeout sets the schedulerEstimatorTimeout for scheduler
func WithSchedulerEstimatorTimeout(schedulerEstimatorTimeout metav1.Duration) Option {
	return func(o *schedulerOptions) {
		o.schedulerEstimatorTimeout = schedulerEstimatorTimeout
	}
}

// WithSchedulerEstimatorServicePrefix sets the schedulerEstimatorServicePrefix for scheduler
func WithSchedulerEstimatorServicePrefix(schedulerEstimatorServicePrefix string) Option {
	return func(o *schedulerOptions) {
		o.schedulerEstimatorServicePrefix = schedulerEstimatorServicePrefix
	}
}

// WithSchedulerName sets the schedulerName for scheduler
func WithSchedulerName(schedulerName string) Option {
	return func(o *schedulerOptions) {
		o.schedulerName = schedulerName
	}
}

// WithEnableEmptyWorkloadPropagation sets the enablePropagateEmptyWorkLoad for scheduler
func WithEnableEmptyWorkloadPropagation(enableEmptyWorkloadPropagation bool) Option {
	return func(o *schedulerOptions) {
		o.enableEmptyWorkloadPropagation = enableEmptyWorkloadPropagation
	}
}

// WithEnableSchedulerPlugin sets the scheduler-plugin for scheduler
func WithEnableSchedulerPlugin(plugins []string) Option {
	return func(o *schedulerOptions) {
		o.plugins = plugins
	}
}

// WithOutOfTreeRegistry sets the registry for out-of-tree plugins. Those plugins
// will be appended to the default in-tree registry.
func WithOutOfTreeRegistry(registry runtime.Registry) Option {
	return func(o *schedulerOptions) {
		o.outOfTreeRegistry = registry
	}
}

// WithRateLimiterOptions sets the rateLimiterOptions for scheduler
func WithRateLimiterOptions(rateLimiterOptions ratelimiterflag.Options) Option {
	return func(o *schedulerOptions) {
		o.RateLimiterOptions = rateLimiterOptions
	}
}

// NewScheduler instantiates a scheduler
func NewScheduler(dynamicClient dynamic.Interface, karmadaClient karmadaclientset.Interface, kubeClient kubernetes.Interface, opts ...Option) (*Scheduler, error) {
	factory := informerfactory.NewSharedInformerFactory(karmadaClient, 0)
	bindingLister := factory.Work().V1alpha2().ResourceBindings().Lister()
	clusterBindingLister := factory.Work().V1alpha2().ClusterResourceBindings().Lister()
	clusterLister := factory.Cluster().V1alpha1().Clusters().Lister()
	schedulerCache := schedulercache.NewCache(clusterLister)

	options := schedulerOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	clock := clock.RealClock{}
	queueName := "scheduler-queue"
	var federatedPriorityClassLister federatedlister.FederatedPriorityClassLister
	var rateLimitingQueueConfig workqueue.RateLimitingQueueConfig
	if options.enablePriorityQueue {
		rateLimitingQueueConfig = workqueue.RateLimitingQueueConfig{
			Name:  queueName,
			Clock: clock,
			DelayingQueue: workqueue.NewDelayingQueueWithConfig(
				workqueue.DelayingQueueConfig{
					Name:  queueName,
					Clock: clock,
					Queue: priorityqueue.NewWithConfig(
						workqueue.QueueConfig{
							Name:  queueName,
							Clock: clock,
						},
					),
				},
			),
		}
		federatedPriorityClassLister = factory.Work().V1alpha1().FederatedPriorityClasses().Lister()
	} else {
		rateLimitingQueueConfig = workqueue.RateLimitingQueueConfig{Name: queueName}
	}
	queue := workqueue.NewRateLimitingQueueWithConfig(
		ratelimiterflag.DefaultControllerRateLimiter(options.RateLimiterOptions),
		rateLimitingQueueConfig,
	)
	registry := frameworkplugins.NewInTreeRegistry()
	if err := registry.Merge(options.outOfTreeRegistry); err != nil {
		return nil, err
	}
	registry = registry.Filter(options.plugins)
	algorithm, err := core.NewGenericScheduler(schedulerCache, registry)
	if err != nil {
		return nil, err
	}

	sched := &Scheduler{
		DynamicClient:                dynamicClient,
		KarmadaClient:                karmadaClient,
		KubeClient:                   kubeClient,
		bindingLister:                bindingLister,
		clusterBindingLister:         clusterBindingLister,
		clusterLister:                clusterLister,
		informerFactory:              factory,
		queue:                        queue,
		Algorithm:                    algorithm,
		schedulerCache:               schedulerCache,
		parallelizer:                 parallelize.NewParallelizer(options.parallelism),
		federatedPriorityClassLister: federatedPriorityClassLister,
	}

	sched.clusterReconcileWorker = util.NewAsyncWorker(util.Options{
		Name:          "ClusterReconcileWorker",
		ReconcileFunc: sched.reconcileCluster,
	})

	if options.enableSchedulerEstimator {
		sched.enableSchedulerEstimator = options.enableSchedulerEstimator
		sched.disableSchedulerEstimatorInPullMode = options.disableSchedulerEstimatorInPullMode
		sched.schedulerEstimatorServicePrefix = options.schedulerEstimatorServicePrefix
		sched.schedulerEstimatorClientConfig = options.schedulerEstimatorClientConfig
		sched.schedulerEstimatorCache = estimatorclient.NewSchedulerEstimatorCache()
		schedulerEstimatorWorkerOptions := util.Options{
			Name:          "scheduler-estimator",
			KeyFunc:       nil,
			ReconcileFunc: sched.reconcileEstimatorConnection,
		}
		sched.schedulerEstimatorServiceNamespace = options.schedulerEstimatorServiceNamespace
		sched.schedulerEstimatorWorker = util.NewAsyncWorker(schedulerEstimatorWorkerOptions)
		schedulerEstimator := estimatorclient.NewSchedulerEstimator(sched.schedulerEstimatorCache, options.schedulerEstimatorTimeout.Duration)
		estimatorclient.RegisterSchedulerEstimator(schedulerEstimator)
	}
	sched.enableEmptyWorkloadPropagation = options.enableEmptyWorkloadPropagation
	sched.schedulerName = options.schedulerName
	sched.enablePriorityQueue = options.enablePriorityQueue

	sched.addAllEventHandlers()
	return sched, nil
}

// Run runs the scheduler
func (s *Scheduler) Run(ctx context.Context) {
	stopCh := ctx.Done()
	klog.Infof("Starting karmada scheduler")
	defer klog.Infof("Shutting down karmada scheduler")

	// Establish all connections first and then begin scheduling.
	if s.enableSchedulerEstimator {
		s.establishEstimatorConnections()
		s.schedulerEstimatorWorker.Run(1, stopCh)
	}

	s.informerFactory.Start(stopCh)
	s.informerFactory.WaitForCacheSync(stopCh)

	s.clusterReconcileWorker.Run(1, stopCh)

	go wait.Until(s.worker, time.Second, stopCh)

	<-stopCh
	s.queue.ShutDown()
}

func (s *Scheduler) worker() {
	for s.scheduleNext() {
	}
}

func (s *Scheduler) scheduleNext() bool {
	item, shutdown := s.queue.Get()
	if shutdown {
		klog.Errorf("Fail to pop item from queue")
		return false
	}
	defer s.queue.Done(item)

	var err error
	if s.enablePriorityQueue {
		err = s.doSchedule(item.(priorityqueue.DataWithPriority).Key)
	} else {
		err = s.doSchedule(item.(string))
	}
	s.handleErr(err, item)
	return true
}

func (s *Scheduler) doSchedule(key string) error {
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if len(ns) > 0 {
		return s.doScheduleBinding(ns, name)
	}
	return s.doScheduleClusterBinding(name)
}

func (s *Scheduler) doScheduleBinding(namespace, name string) (err error) {
	rb, err := s.bindingLister.ResourceBindings(namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// the binding does not exist, do nothing
			return nil
		}
		return err
	}
	if !rb.DeletionTimestamp.IsZero() {
		s.recordScheduleResultEventForResourceBinding(rb, nil, fmt.Errorf("skip schedule deleting resourceBinding: %s/%s", rb.Namespace, rb.Name))
		klog.V(4).InfoS("Skip schedule deleting ResourceBinding", "ResourceBinding", klog.KObj(rb))
		return nil
	}

	rb = rb.DeepCopy()

	if rb.Spec.Placement == nil {
		// never reach here
		err = fmt.Errorf("failed to get placement from resourceBinding(%s/%s)", rb.Namespace, rb.Name)
		klog.Error(err)
		return err
	}

	start := time.Now()
	appliedPlacementStr := util.GetLabelValue(rb.Annotations, util.PolicyPlacementAnnotation)
	if placementChanged(*rb.Spec.Placement, appliedPlacementStr, rb.Status.SchedulerObservedAffinityName) {
		// policy placement changed, need schedule
		klog.Infof("Start to schedule ResourceBinding(%s/%s) as placement changed", namespace, name)
		err = s.scheduleResourceBinding(rb)
		metrics.BindingSchedule(string(ReconcileSchedule), utilmetrics.DurationInSeconds(start), err)
		return err
	}
	if util.IsBindingReplicasChanged(&rb.Spec, rb.Spec.Placement.ReplicaScheduling) {
		// binding replicas changed, need reschedule
		klog.Infof("Reschedule ResourceBinding(%s/%s) as replicas scaled down or scaled up", namespace, name)
		err = s.scheduleResourceBinding(rb)
		metrics.BindingSchedule(string(ScaleSchedule), utilmetrics.DurationInSeconds(start), err)
		return err
	}
	if util.RescheduleRequired(rb.Spec.RescheduleTriggeredAt, rb.Status.LastScheduledTime) {
		// explicitly triggered reschedule
		klog.Infof("Reschedule ResourceBinding(%s/%s) as explicitly triggered reschedule", namespace, name)
		err = s.scheduleResourceBinding(rb)
		metrics.BindingSchedule(string(ReconcileSchedule), utilmetrics.DurationInSeconds(start), err)
		return err
	}
	if rb.Spec.Replicas == 0 ||
		rb.Spec.Placement.ReplicaSchedulingType() == policyv1alpha1.ReplicaSchedulingTypeDuplicated {
		// Duplicated resources should always be scheduled. Note: non-workload is considered as duplicated
		// even if scheduling type is divided.
		klog.V(3).Infof("Start to schedule ResourceBinding(%s/%s) as scheduling type is duplicated", namespace, name)
		err = s.scheduleResourceBinding(rb)
		metrics.BindingSchedule(string(ReconcileSchedule), utilmetrics.DurationInSeconds(start), err)
		return err
	}
	// TODO(dddddai): reschedule bindings on cluster change
	klog.V(3).Infof("Don't need to schedule ResourceBinding(%s/%s)", rb.Namespace, rb.Name)

	// If no scheduling is required, we need to ensure that binding.Generation is equal to
	// binding.Status.SchedulerObservedGeneration which means the current status of binding
	// is the latest status of successful scheduling.
	if rb.Generation != rb.Status.SchedulerObservedGeneration {
		updateRB := rb.DeepCopy()
		updateRB.Status.SchedulerObservedGeneration = updateRB.Generation
		return patchBindingStatus(s.KarmadaClient, rb, updateRB)
	}
	return nil
}

func (s *Scheduler) doScheduleClusterBinding(name string) (err error) {
	crb, err := s.clusterBindingLister.Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// the binding does not exist, do nothing
			return nil
		}
		return err
	}
	if !crb.DeletionTimestamp.IsZero() {
		s.recordScheduleResultEventForClusterResourceBinding(crb, nil, fmt.Errorf("skip schedule deleting clusterResourceBinding: %s", crb.Name))
		klog.V(4).InfoS("Skip schedule deleting ClusterResourceBinding", "ClusterResourceBinding", klog.KObj(crb))
		return nil
	}

	crb = crb.DeepCopy()

	if crb.Spec.Placement == nil {
		// never reach here
		err = fmt.Errorf("failed to get placement from clusterResourceBinding(%s)", crb.Name)
		klog.Error(err)
		return err
	}

	start := time.Now()
	appliedPlacementStr := util.GetLabelValue(crb.Annotations, util.PolicyPlacementAnnotation)
	if placementChanged(*crb.Spec.Placement, appliedPlacementStr, crb.Status.SchedulerObservedAffinityName) {
		// policy placement changed, need schedule
		klog.Infof("Start to schedule ClusterResourceBinding(%s) as placement changed", name)
		err = s.scheduleClusterResourceBinding(crb)
		metrics.BindingSchedule(string(ReconcileSchedule), utilmetrics.DurationInSeconds(start), err)
		return err
	}
	if util.IsBindingReplicasChanged(&crb.Spec, crb.Spec.Placement.ReplicaScheduling) {
		// binding replicas changed, need reschedule
		klog.Infof("Reschedule ClusterResourceBinding(%s) as replicas scaled down or scaled up", name)
		err = s.scheduleClusterResourceBinding(crb)
		metrics.BindingSchedule(string(ScaleSchedule), utilmetrics.DurationInSeconds(start), err)
		return err
	}
	if util.RescheduleRequired(crb.Spec.RescheduleTriggeredAt, crb.Status.LastScheduledTime) {
		// explicitly triggered reschedule
		klog.Infof("Start to schedule ClusterResourceBinding(%s) as explicitly triggered reschedule", name)
		err = s.scheduleClusterResourceBinding(crb)
		metrics.BindingSchedule(string(ReconcileSchedule), utilmetrics.DurationInSeconds(start), err)
		return err
	}
	if crb.Spec.Replicas == 0 ||
		crb.Spec.Placement.ReplicaSchedulingType() == policyv1alpha1.ReplicaSchedulingTypeDuplicated {
		// Duplicated resources should always be scheduled. Note: non-workload is considered as duplicated
		// even if scheduling type is divided.
		klog.V(3).Infof("Start to schedule ClusterResourceBinding(%s) as scheduling type is duplicated", name)
		err = s.scheduleClusterResourceBinding(crb)
		metrics.BindingSchedule(string(ReconcileSchedule), utilmetrics.DurationInSeconds(start), err)
		return err
	}
	// TODO(dddddai): reschedule bindings on cluster change
	klog.Infof("Don't need to schedule ClusterResourceBinding(%s)", name)

	// If no scheduling is required, we need to ensure that binding.Generation is equal to
	// binding.Status.SchedulerObservedGeneration which means the current status of binding
	// is the latest status of successful scheduling.
	if crb.Generation != crb.Status.SchedulerObservedGeneration {
		updateCRB := crb.DeepCopy()
		updateCRB.Status.SchedulerObservedGeneration = updateCRB.Generation
		return patchClusterResourceBindingStatus(s.KarmadaClient, crb, updateCRB)
	}
	return nil
}

func (s *Scheduler) scheduleResourceBinding(rb *workv1alpha2.ResourceBinding) (err error) {
	defer func() {
		condition, ignoreErr := getConditionByError(err)
		if updateErr := patchBindingStatusCondition(s.KarmadaClient, rb, condition); updateErr != nil {
			// if patch error occurs, just return patch error to reconcile again.
			err = updateErr
			klog.Errorf("Failed to patch schedule status to ResourceBinding(%s/%s): %v", rb.Namespace, rb.Name, err)
		} else if ignoreErr && err != nil {
			// for finished schedule, we won't retry.
			err = nil
		}
	}()

	if rb.Spec.Placement.ClusterAffinities != nil {
		return s.scheduleResourceBindingWithClusterAffinities(rb)
	}
	return s.scheduleResourceBindingWithClusterAffinity(rb)
}

func (s *Scheduler) scheduleResourceBindingWithClusterAffinity(rb *workv1alpha2.ResourceBinding) error {
	klog.V(4).InfoS("Begin scheduling resource binding with ClusterAffinity", "resourceBinding", klog.KObj(rb))
	defer klog.V(4).InfoS("End scheduling resource binding with ClusterAffinity", "resourceBinding", klog.KObj(rb))

	placementBytes, err := json.Marshal(*rb.Spec.Placement)
	if err != nil {
		klog.V(4).ErrorS(err, "Failed to marshal binding placement", "resourceBinding", klog.KObj(rb))
		return err
	}

	scheduleResult, err := s.Algorithm.Schedule(context.TODO(), &rb.Spec, &rb.Status, &core.ScheduleAlgorithmOption{EnableEmptyWorkloadPropagation: s.enableEmptyWorkloadPropagation})
	var fitErr *framework.FitError
	// in case of no cluster error, can not return but continue to patch(cleanup) the result.
	if err != nil && !errors.As(err, &fitErr) {
		klog.Infoln("feasible clusters: ", scheduleResult.FeasibleClusters)
		if s.Algorithm.HasPostFilterPlugins() && len(scheduleResult.FeasibleClusters) > 0 {
			klog.Infoln("There are registered PostFilter plugins, so perform preemption")
			testFlag := false
			var preemptionTargets *framework.PreemptionTargets
			var preemptionErr error

			if testFlag {
				var selector labels.Selector
				if selector, preemptionErr = metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"test-preemption-label": "should-be-preempt"}}); preemptionErr != nil {
					klog.Error("Find selector error", preemptionErr)
					return preemptionErr
				}
				bindings, preemptionErr := s.bindingLister.ResourceBindings(rb.Namespace).List(selector)
				if preemptionErr != nil {
					klog.Error("Find preemptee bindings error", preemptionErr)
					return preemptionErr
				}
				targetBindings := make([]workv1alpha2.ObjectReference, len(bindings))
				for i, binding := range bindings {
					klog.Infof("Find preemptee binding: %v", binding)
					targetBinding := workv1alpha2.ObjectReference{
						APIVersion: rb.APIVersion,
						Kind:       workv1alpha2.ResourceKindResourceBinding,
						Namespace:  binding.Namespace,
						Name:       binding.Name,
						UID:        binding.UID,
					}
					targetBindings[i] = targetBinding
				}
				preemptionTargets = &framework.PreemptionTargets{
					TargetBindings: targetBindings,
				}
			} else {
				preemptionTargets, preemptionErr = s.Algorithm.FindPreemptionTargets(context.TODO(), &rb.Spec, scheduleResult.FeasibleClusters)
				if preemptionErr != nil {
					klog.Errorf("Failed finding preemption targets forResourceBinding(%s/%s): %v", rb.Namespace, rb.Name, err)
					err = utilerrors.NewAggregate([]error{err, preemptionErr})
				}
			}

			klog.Infof("Find preemptionTargets targetbindings: %v", preemptionTargets.TargetBindings)
			if len(preemptionTargets.TargetBindings) > 0 {
				preemptionErr := s.issuePreemption(workv1alpha2.ObjectReference{
					APIVersion: rb.APIVersion,
					Kind:       rb.Kind,
					Namespace:  rb.GetNamespace(),
					Name:       rb.GetName(),
					UID:        rb.GetUID(),
				}, preemptionTargets.TargetBindings)
				if preemptionErr != nil {
					err = utilerrors.NewAggregate([]error{err, preemptionErr})
				}
			}
		}

		s.recordScheduleResultEventForResourceBinding(rb, nil, err)
		klog.Errorf("Failed scheduling ResourceBinding(%s/%s): %v", rb.Namespace, rb.Name, err)
		return err
	}

	klog.V(4).Infof("ResourceBinding(%s/%s) scheduled to clusters %v", rb.Namespace, rb.Name, scheduleResult.SuggestedClusters)
	patchErr := s.patchScheduleResultForResourceBinding(rb, string(placementBytes), scheduleResult.SuggestedClusters)
	if patchErr != nil {
		err = utilerrors.NewAggregate([]error{err, patchErr})
	}
	s.recordScheduleResultEventForResourceBinding(rb, scheduleResult.SuggestedClusters, err)
	return err
}

func (s *Scheduler) scheduleResourceBindingWithClusterAffinities(rb *workv1alpha2.ResourceBinding) error {
	klog.V(4).InfoS("Begin scheduling resourceBinding with ClusterAffinities", "resourceBinding", klog.KObj(rb))
	defer klog.V(4).InfoS("End scheduling resourceBinding with ClusterAffinities", "resourceBinding", klog.KObj(rb))

	placementBytes, err := json.Marshal(*rb.Spec.Placement)
	if err != nil {
		klog.V(4).ErrorS(err, "Failed to marshal binding placement", "resourceBinding", klog.KObj(rb))
		return err
	}

	var (
		scheduleResult core.ScheduleResult
		firstErr       error
	)

	affinityIndex := getAffinityIndex(rb.Spec.Placement.ClusterAffinities, rb.Status.SchedulerObservedAffinityName)
	updatedStatus := rb.Status.DeepCopy()
	for affinityIndex < len(rb.Spec.Placement.ClusterAffinities) {
		klog.V(4).Infof("Schedule ResourceBinding(%s/%s) with clusterAffiliates index(%d)", rb.Namespace, rb.Name, affinityIndex)
		updatedStatus.SchedulerObservedAffinityName = rb.Spec.Placement.ClusterAffinities[affinityIndex].AffinityName
		scheduleResult, err = s.Algorithm.Schedule(context.TODO(), &rb.Spec, updatedStatus, &core.ScheduleAlgorithmOption{EnableEmptyWorkloadPropagation: s.enableEmptyWorkloadPropagation})
		if err == nil {
			break
		}

		// obtain to err of the first scheduling
		if firstErr == nil {
			firstErr = err
		}

		err = fmt.Errorf("failed to schedule ResourceBinding(%s/%s) with clusterAffiliates index(%d): %v", rb.Namespace, rb.Name, affinityIndex, err)
		klog.Error(err)
		s.recordScheduleResultEventForResourceBinding(rb, nil, err)
		affinityIndex++
	}

	if affinityIndex >= len(rb.Spec.Placement.ClusterAffinities) {
		klog.Errorf("Failed to schedule ResourceBinding(%s/%s) with all ClusterAffinities.", rb.Namespace, rb.Name)

		updatedStatus.SchedulerObservedAffinityName = rb.Status.SchedulerObservedAffinityName

		var fitErr *framework.FitError
		if !errors.As(firstErr, &fitErr) {
			return firstErr
		}

		klog.V(4).Infof("ResourceBinding(%s/%s) scheduled to clusters %v", rb.Namespace, rb.Name, nil)
		patchErr := s.patchScheduleResultForResourceBinding(rb, string(placementBytes), nil)
		if patchErr != nil {
			err = utilerrors.NewAggregate([]error{firstErr, patchErr})
		} else {
			err = firstErr
		}
		s.recordScheduleResultEventForResourceBinding(rb, nil, err)
		return err
	}

	klog.V(4).Infof("ResourceBinding(%s/%s) scheduled to clusters %v", rb.Namespace, rb.Name, scheduleResult.SuggestedClusters)
	patchErr := s.patchScheduleResultForResourceBinding(rb, string(placementBytes), scheduleResult.SuggestedClusters)
	patchStatusErr := patchBindingStatusWithAffinityName(s.KarmadaClient, rb, updatedStatus.SchedulerObservedAffinityName)
	scheduleErr := utilerrors.NewAggregate([]error{patchErr, patchStatusErr})
	s.recordScheduleResultEventForResourceBinding(rb, nil, scheduleErr)
	return scheduleErr
}

func (s *Scheduler) patchScheduleResultForResourceBinding(oldBinding *workv1alpha2.ResourceBinding, placement string, scheduleResult []workv1alpha2.TargetCluster) error {
	newBinding := oldBinding.DeepCopy()
	if newBinding.Annotations == nil {
		newBinding.Annotations = make(map[string]string)
	}
	newBinding.Annotations[util.PolicyPlacementAnnotation] = placement
	newBinding.Spec.Clusters = scheduleResult

	patchBytes, err := helper.GenMergePatch(oldBinding, newBinding)
	if err != nil {
		return err
	}
	if len(patchBytes) == 0 {
		return nil
	}

	_, err = s.KarmadaClient.WorkV1alpha2().ResourceBindings(newBinding.Namespace).Patch(context.TODO(), newBinding.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("Failed to patch schedule to ResourceBinding(%s/%s): %v", oldBinding.Namespace, oldBinding.Name, err)
		return err
	}

	klog.V(4).Infof("Patch schedule to ResourceBinding(%s/%s) succeed", oldBinding.Namespace, oldBinding.Name)
	return nil
}

func (s *Scheduler) scheduleClusterResourceBinding(crb *workv1alpha2.ClusterResourceBinding) (err error) {
	defer func() {
		condition, ignoreErr := getConditionByError(err)
		if updateErr := patchClusterBindingStatusCondition(s.KarmadaClient, crb, condition); updateErr != nil {
			// if patch error occurs, just return patch error to reconcile again.
			err = updateErr
			klog.Errorf("Failed to patch schedule status to ClusterResourceBinding(%s): %v", crb.Name, err)
		} else if ignoreErr && err != nil {
			// for finished schedule, we won't retry.
			err = nil
		}
	}()

	if crb.Spec.Placement.ClusterAffinities != nil {
		return s.scheduleClusterResourceBindingWithClusterAffinities(crb)
	}
	return s.scheduleClusterResourceBindingWithClusterAffinity(crb)
}

func (s *Scheduler) scheduleClusterResourceBindingWithClusterAffinity(crb *workv1alpha2.ClusterResourceBinding) error {
	klog.V(4).InfoS("Begin scheduling clusterResourceBinding with ClusterAffinity", "clusterResourceBinding", klog.KObj(crb))
	defer klog.V(4).InfoS("End scheduling clusterResourceBinding with ClusterAffinity", "clusterResourceBinding", klog.KObj(crb))

	placementBytes, err := json.Marshal(*crb.Spec.Placement)
	if err != nil {
		klog.V(4).ErrorS(err, "Failed to marshal binding placement", "clusterResourceBinding", klog.KObj(crb))
		return err
	}

	scheduleResult, err := s.Algorithm.Schedule(context.TODO(), &crb.Spec, &crb.Status, &core.ScheduleAlgorithmOption{EnableEmptyWorkloadPropagation: s.enableEmptyWorkloadPropagation})
	var fitErr *framework.FitError
	// in case of no cluster error, can not return but continue to patch(cleanup) the result.
	if err != nil && !errors.As(err, &fitErr) {
		s.recordScheduleResultEventForClusterResourceBinding(crb, nil, err)
		klog.Errorf("Failed scheduling clusterResourceBinding(%s): %v", crb.Name, err)
		return err
	}

	klog.V(4).Infof("clusterResourceBinding(%s) scheduled to clusters %v", crb.Name, scheduleResult.SuggestedClusters)
	patchErr := s.patchScheduleResultForClusterResourceBinding(crb, string(placementBytes), scheduleResult.SuggestedClusters)
	if patchErr != nil {
		err = utilerrors.NewAggregate([]error{err, patchErr})
	}
	s.recordScheduleResultEventForClusterResourceBinding(crb, scheduleResult.SuggestedClusters, err)
	return err
}

func (s *Scheduler) scheduleClusterResourceBindingWithClusterAffinities(crb *workv1alpha2.ClusterResourceBinding) error {
	klog.V(4).InfoS("Begin scheduling clusterResourceBinding with ClusterAffinities", "clusterResourceBinding", klog.KObj(crb))
	defer klog.V(4).InfoS("End scheduling clusterResourceBinding with ClusterAffinities", "clusterResourceBinding", klog.KObj(crb))

	placementBytes, err := json.Marshal(*crb.Spec.Placement)
	if err != nil {
		klog.V(4).ErrorS(err, "Failed to marshal binding placement", "clusterResourceBinding", klog.KObj(crb))
		return err
	}

	var (
		scheduleResult core.ScheduleResult
		firstErr       error
	)

	affinityIndex := getAffinityIndex(crb.Spec.Placement.ClusterAffinities, crb.Status.SchedulerObservedAffinityName)
	updatedStatus := crb.Status.DeepCopy()
	for affinityIndex < len(crb.Spec.Placement.ClusterAffinities) {
		klog.V(4).Infof("Schedule ClusterResourceBinding(%s) with clusterAffiliates index(%d)", crb.Name, affinityIndex)
		updatedStatus.SchedulerObservedAffinityName = crb.Spec.Placement.ClusterAffinities[affinityIndex].AffinityName
		scheduleResult, err = s.Algorithm.Schedule(context.TODO(), &crb.Spec, updatedStatus, &core.ScheduleAlgorithmOption{EnableEmptyWorkloadPropagation: s.enableEmptyWorkloadPropagation})
		if err == nil {
			break
		}

		// obtain to err of the first scheduling
		if firstErr == nil {
			firstErr = err
		}

		err = fmt.Errorf("failed to schedule ClusterResourceBinding(%s) with clusterAffiliates index(%d): %v", crb.Name, affinityIndex, err)
		klog.Error(err)
		s.recordScheduleResultEventForClusterResourceBinding(crb, nil, err)
		affinityIndex++
	}

	if affinityIndex >= len(crb.Spec.Placement.ClusterAffinities) {
		klog.Errorf("Failed to schedule ClusterResourceBinding(%s) with all ClusterAffinities.", crb.Name)

		updatedStatus.SchedulerObservedAffinityName = crb.Status.SchedulerObservedAffinityName

		var fitErr *framework.FitError
		if !errors.As(firstErr, &fitErr) {
			return firstErr
		}

		klog.V(4).Infof("ClusterResourceBinding(%s) scheduled to clusters %v", crb.Name, nil)
		patchErr := s.patchScheduleResultForClusterResourceBinding(crb, string(placementBytes), nil)
		if patchErr != nil {
			err = utilerrors.NewAggregate([]error{firstErr, patchErr})
		} else {
			err = firstErr
		}
		s.recordScheduleResultEventForClusterResourceBinding(crb, nil, err)
		return err
	}

	klog.V(4).Infof("ClusterResourceBinding(%s) scheduled to clusters %v", crb.Name, scheduleResult.SuggestedClusters)
	patchErr := s.patchScheduleResultForClusterResourceBinding(crb, string(placementBytes), scheduleResult.SuggestedClusters)
	patchStatusErr := patchClusterBindingStatusWithAffinityName(s.KarmadaClient, crb, updatedStatus.SchedulerObservedAffinityName)
	scheduleErr := utilerrors.NewAggregate([]error{patchErr, patchStatusErr})
	s.recordScheduleResultEventForClusterResourceBinding(crb, nil, scheduleErr)
	return scheduleErr
}

func (s *Scheduler) patchScheduleResultForClusterResourceBinding(oldBinding *workv1alpha2.ClusterResourceBinding, placement string, scheduleResult []workv1alpha2.TargetCluster) error {
	newBinding := oldBinding.DeepCopy()
	if newBinding.Annotations == nil {
		newBinding.Annotations = make(map[string]string)
	}
	newBinding.Annotations[util.PolicyPlacementAnnotation] = placement
	newBinding.Spec.Clusters = scheduleResult

	patchBytes, err := helper.GenMergePatch(oldBinding, newBinding)
	if err != nil {
		return err
	}
	if len(patchBytes) == 0 {
		return nil
	}

	_, err = s.KarmadaClient.WorkV1alpha2().ClusterResourceBindings().Patch(context.TODO(), newBinding.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("Failed to patch schedule to ClusterResourceBinding(%s): %v", oldBinding.Name, err)
		return err
	}

	klog.V(4).Infof("Patch schedule to ClusterResourceBinding(%s) succeed", oldBinding.Name)
	return nil
}

func (s *Scheduler) handleErr(err error, key interface{}) {
	if err == nil || apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
		s.queue.Forget(key)
		return
	}

	s.queue.AddAfter(key, 60*time.Second)
	// s.queue.AddRateLimited(key)
	metrics.CountSchedulerBindings(metrics.ScheduleAttemptFailure)
}

func (s *Scheduler) reconcileEstimatorConnection(key util.QueueKey) error {
	name, ok := key.(string)
	if !ok {
		return fmt.Errorf("failed to reconcile estimator connection as invalid key: %v", key)
	}
	klog.Infoln("reconcileEstimatorConnection")

	cluster, err := s.clusterLister.Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			s.schedulerEstimatorCache.DeleteCluster(name)
			return nil
		}
		return err
	}
	if cluster.Spec.SyncMode == clusterv1alpha1.Pull && s.disableSchedulerEstimatorInPullMode {
		return nil
	}
	klog.Infoln("clusterLister")

	serviceInfo := estimatorclient.SchedulerEstimatorServiceInfo{
		Name:       name,
		Namespace:  s.schedulerEstimatorServiceNamespace,
		NamePrefix: s.schedulerEstimatorServicePrefix,
	}
	return estimatorclient.EstablishConnection(s.KubeClient, serviceInfo, s.schedulerEstimatorCache, s.schedulerEstimatorClientConfig)
}

func (s *Scheduler) establishEstimatorConnections() {
	clusterList, err := s.KarmadaClient.ClusterV1alpha1().Clusters().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Cannot list all clusters when establish all cluster estimator connections: %v", err)
		return
	}
	for i := range clusterList.Items {
		if clusterList.Items[i].Spec.SyncMode == clusterv1alpha1.Pull && s.disableSchedulerEstimatorInPullMode {
			continue
		}
		serviceInfo := estimatorclient.SchedulerEstimatorServiceInfo{
			Name:       clusterList.Items[i].Name,
			Namespace:  s.schedulerEstimatorServiceNamespace,
			NamePrefix: s.schedulerEstimatorServicePrefix,
		}
		klog.Infoln("establishEstimatorConnections")

		if err = estimatorclient.EstablishConnection(s.KubeClient, serviceInfo, s.schedulerEstimatorCache, s.schedulerEstimatorClientConfig); err != nil {
			klog.Error(err)
		}
	}
}

// patchBindingStatusCondition patches schedule status condition of ResourceBinding when necessary.
func patchBindingStatusCondition(karmadaClient karmadaclientset.Interface, rb *workv1alpha2.ResourceBinding, newScheduledCondition metav1.Condition) error {
	klog.V(4).Infof("Begin to patch status condition to ResourceBinding(%s/%s)", rb.Namespace, rb.Name)

	updateRB := rb.DeepCopy()
	meta.SetStatusCondition(&updateRB.Status.Conditions, newScheduledCondition)
	// Postpone setting observed generation until schedule succeed, assume scheduler will retry and
	// will succeed eventually.
	if newScheduledCondition.Status == metav1.ConditionTrue {
		updateRB.Status.SchedulerObservedGeneration = rb.Generation
		currentTime := metav1.Now()
		updateRB.Status.LastScheduledTime = &currentTime
	}

	if reflect.DeepEqual(rb.Status, updateRB.Status) {
		return nil
	}
	return patchBindingStatus(karmadaClient, rb, updateRB)
}

// patchBindingStatusWithAffinityName patches schedule status with affinityName of ResourceBinding when necessary.
func patchBindingStatusWithAffinityName(karmadaClient karmadaclientset.Interface, rb *workv1alpha2.ResourceBinding, affinityName string) error {
	if rb.Status.SchedulerObservedAffinityName == affinityName {
		return nil
	}

	klog.V(4).Infof("Begin to patch status with affinityName(%s) to ResourceBinding(%s/%s).", affinityName, rb.Namespace, rb.Name)
	updateRB := rb.DeepCopy()
	updateRB.Status.SchedulerObservedAffinityName = affinityName
	return patchBindingStatus(karmadaClient, rb, updateRB)
}

func patchBindingStatus(karmadaClient karmadaclientset.Interface, rb, updateRB *workv1alpha2.ResourceBinding) error {
	patchBytes, err := helper.GenFieldMergePatch("status", rb.Status, updateRB.Status)
	if err != nil {
		return err
	}
	if len(patchBytes) == 0 {
		return nil
	}

	_, err = karmadaClient.WorkV1alpha2().ResourceBindings(rb.Namespace).Patch(context.TODO(), rb.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if err != nil {
		klog.Errorf("Failed to patch schedule status ResourceBinding(%s/%s): %v", rb.Namespace, rb.Name, err)
		return err
	}

	klog.V(4).Infof("Patch schedule status to ResourceBinding(%s/%s) succeed", rb.Namespace, rb.Name)
	return nil
}

// patchClusterBindingStatusCondition patches schedule status condition of ClusterResourceBinding when necessary
func patchClusterBindingStatusCondition(karmadaClient karmadaclientset.Interface, crb *workv1alpha2.ClusterResourceBinding, newScheduledCondition metav1.Condition) error {
	klog.V(4).Infof("Begin to patch status condition to ClusterResourceBinding(%s)", crb.Name)

	updateCRB := crb.DeepCopy()
	meta.SetStatusCondition(&updateCRB.Status.Conditions, newScheduledCondition)
	// Postpone setting observed generation until schedule succeed, assume scheduler will retry and
	// will succeed eventually.
	if newScheduledCondition.Status == metav1.ConditionTrue {
		updateCRB.Status.SchedulerObservedGeneration = crb.Generation
		currentTime := metav1.Now()
		updateCRB.Status.LastScheduledTime = &currentTime
	}

	if reflect.DeepEqual(crb.Status, updateCRB.Status) {
		return nil
	}
	return patchClusterResourceBindingStatus(karmadaClient, crb, updateCRB)
}

// patchClusterBindingStatusWithAffinityName patches schedule status with affinityName of ClusterResourceBinding when necessary.
func patchClusterBindingStatusWithAffinityName(karmadaClient karmadaclientset.Interface, crb *workv1alpha2.ClusterResourceBinding, affinityName string) error {
	if crb.Status.SchedulerObservedAffinityName == affinityName {
		return nil
	}

	klog.V(4).Infof("Begin to patch status with affinityName(%s) to ClusterResourceBinding(%s).", affinityName, crb.Name)
	updateCRB := crb.DeepCopy()
	updateCRB.Status.SchedulerObservedAffinityName = affinityName
	return patchClusterResourceBindingStatus(karmadaClient, crb, updateCRB)
}

func patchClusterResourceBindingStatus(karmadaClient karmadaclientset.Interface, crb, updateCRB *workv1alpha2.ClusterResourceBinding) error {
	patchBytes, err := helper.GenFieldMergePatch("status", crb.Status, updateCRB.Status)
	if err != nil {
		return err
	}
	if len(patchBytes) == 0 {
		return nil
	}

	_, err = karmadaClient.WorkV1alpha2().ClusterResourceBindings().Patch(context.TODO(), crb.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if err != nil {
		klog.Errorf("Failed to patch schedule status to ClusterResourceBinding(%s): %v", crb.Name, err)
		return err
	}

	klog.V(4).Infof("Patch schedule status to ClusterResourceBinding(%s) succeed", crb.Name)
	return nil
}

func (s *Scheduler) recordScheduleResultEventForResourceBinding(rb *workv1alpha2.ResourceBinding,
	scheduleResult []workv1alpha2.TargetCluster, schedulerErr error) {
	if rb == nil {
		return
	}

	ref := &corev1.ObjectReference{
		Kind:       rb.Spec.Resource.Kind,
		APIVersion: rb.Spec.Resource.APIVersion,
		Namespace:  rb.Spec.Resource.Namespace,
		Name:       rb.Spec.Resource.Name,
		UID:        rb.Spec.Resource.UID,
	}

	if schedulerErr == nil {
		successMsg := fmt.Sprintf("%s Result: {%s}", successfulSchedulingMessage, targetClustersToString(scheduleResult))
		s.eventRecorder.Event(rb, corev1.EventTypeNormal, events.EventReasonScheduleBindingSucceed, successMsg)
		s.eventRecorder.Event(ref, corev1.EventTypeNormal, events.EventReasonScheduleBindingSucceed, successMsg)
	} else {
		s.eventRecorder.Event(rb, corev1.EventTypeWarning, events.EventReasonScheduleBindingFailed, schedulerErr.Error())
		s.eventRecorder.Event(ref, corev1.EventTypeWarning, events.EventReasonScheduleBindingFailed, schedulerErr.Error())
	}
}

func (s *Scheduler) recordScheduleResultEventForClusterResourceBinding(crb *workv1alpha2.ClusterResourceBinding,
	scheduleResult []workv1alpha2.TargetCluster, schedulerErr error) {
	if crb == nil {
		return
	}

	ref := &corev1.ObjectReference{
		Kind:       crb.Spec.Resource.Kind,
		APIVersion: crb.Spec.Resource.APIVersion,
		Namespace:  crb.Spec.Resource.Namespace,
		Name:       crb.Spec.Resource.Name,
		UID:        crb.Spec.Resource.UID,
	}

	if schedulerErr == nil {
		successMsg := fmt.Sprintf("%s Result {%s}", successfulSchedulingMessage, targetClustersToString(scheduleResult))
		s.eventRecorder.Event(crb, corev1.EventTypeNormal, events.EventReasonScheduleBindingSucceed, successMsg)
		s.eventRecorder.Event(ref, corev1.EventTypeNormal, events.EventReasonScheduleBindingSucceed, successMsg)
	} else {
		s.eventRecorder.Event(crb, corev1.EventTypeWarning, events.EventReasonScheduleBindingFailed, schedulerErr.Error())
		s.eventRecorder.Event(ref, corev1.EventTypeWarning, events.EventReasonScheduleBindingFailed, schedulerErr.Error())
	}
}

// targetClustersToString convert []workv1alpha2.TargetCluster to string in format like "member:1, member2:2".
func targetClustersToString(tcs []workv1alpha2.TargetCluster) string {
	tcsStrs := make([]string, 0, len(tcs))
	for _, cluster := range tcs {
		tcsStrs = append(tcsStrs, fmt.Sprintf("%s:%d", cluster.Name, cluster.Replicas))
	}
	return strings.Join(tcsStrs, ", ")
}

func (s *Scheduler) issuePreemption(preemptorBinding workv1alpha2.ObjectReference, preemptionTargets []workv1alpha2.ObjectReference) error {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	errCh := parallelize.NewErrorChannel()
	klog.Infoln("preemption start")
	preemptBindings := func(index int) {
		target := preemptionTargets[index]

		klog.Infof("target: %v", target)
		switch target.Kind {
		case workv1alpha2.ResourceKindResourceBinding:
			resourceBinding, err := s.KarmadaClient.WorkV1alpha2().ResourceBindings(target.Namespace).Get(ctx, target.Name, metav1.GetOptions{})
			if err != nil {
				errCh.SendErrorWithCancel(err, cancel)
				return
			}
			if err := s.KarmadaClient.WorkV1alpha2().ResourceBindings(target.Namespace).Delete(ctx, target.Name, metav1.DeleteOptions{}); err != nil {
				errCh.SendErrorWithCancel(err, cancel)
				return
			}
			ref := &corev1.ObjectReference{
				Kind:       resourceBinding.Spec.Resource.Kind,
				APIVersion: resourceBinding.Spec.Resource.APIVersion,
				Namespace:  resourceBinding.Spec.Resource.Namespace,
				Name:       resourceBinding.Spec.Resource.Name,
				UID:        resourceBinding.Spec.Resource.UID,
			}

			s.eventRecorder.Event(resourceBinding, corev1.EventTypeWarning, "Preempted", fmt.Sprintf("Preempted by resourceBinding %v", preemptorBinding))
			s.eventRecorder.Event(ref, corev1.EventTypeWarning, "Preempted", fmt.Sprintf("Preempted by resourceBinding %v", preemptorBinding))

		case workv1alpha2.ResourceKindClusterResourceBinding:
			clusterResourceBinding, err := s.KarmadaClient.WorkV1alpha2().ClusterResourceBindings().Get(ctx, target.Name, metav1.GetOptions{})
			if err != nil {
				errCh.SendErrorWithCancel(err, cancel)
				return
			}
			if err := s.KarmadaClient.WorkV1alpha2().ClusterResourceBindings().Delete(ctx, target.Name, metav1.DeleteOptions{}); err != nil {
				errCh.SendErrorWithCancel(err, cancel)
				return
			}
			ref := &corev1.ObjectReference{
				Kind:       clusterResourceBinding.Spec.Resource.Kind,
				APIVersion: clusterResourceBinding.Spec.Resource.APIVersion,
				Namespace:  clusterResourceBinding.Spec.Resource.Namespace,
				Name:       clusterResourceBinding.Spec.Resource.Name,
				UID:        clusterResourceBinding.Spec.Resource.UID,
			}

			s.eventRecorder.Event(clusterResourceBinding, corev1.EventTypeWarning, "Preempted", fmt.Sprintf("Preempted by clusterResourceBinding %v", clusterResourceBinding))
			s.eventRecorder.Event(ref, corev1.EventTypeWarning, "Preempted", fmt.Sprintf("Preempted by object %v", ref))

		default:
			errCh.SendErrorWithCancel(fmt.Errorf("unsupported binding kind: %s", target.Kind), cancel)
			return
		}
	}

	s.parallelizer.Until(ctx, len(preemptionTargets), preemptBindings)
	return errCh.ReceiveError()
}
