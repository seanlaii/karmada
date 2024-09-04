package defaultpreemption

import (
	"context"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/scheduler/framework"
)

const (
	// Name is the name of the plugin used in the plugin registry and configurations.
	Name = "AccuratePreemption"
)

// APIEnablement is a plugin that checks if the API(CRD) of the resource is installed in the target cluster.
type AccuratePreemption struct{}

var _ framework.PostFilterPlugin = &AccuratePreemption{}

// New instantiates the APIEnablement plugin.
func New() (framework.Plugin, error) {
	return &AccuratePreemption{}, nil
}

// Name returns the plugin name.
func (p *AccuratePreemption) Name() string {
	return Name
}

func (p *AccuratePreemption) PostFilter(
	ctx context.Context,
	bindingSpec *workv1alpha2.ResourceBindingSpec,
	clusters []*clusterv1alpha1.Cluster,
) (*framework.PreemptionTargets, *framework.Result) {
	var targetVictims []*workv1alpha2.ObjectReference

	// estimators := estimatorclient.GetReplicaEstimators()
	// for name, estimator := range estimators {
	// 	victimCandidates, err := estimator.GetVictimResourceBindings(ctx, clusters, bindingSpec.ReplicaRequirements)
	// 	if err != nil {
	// 		klog.Errorf("Max cluster available replicas error: %v, estimator name: %v", err, name)
	// 		continue
	// 	}
	// 	if len(victimCandidates) < len(targetVictims) {
	// 		targetVictims = victimCandidates
	// 	}
	// }
	return &framework.PreemptionTargets{
		TargetBindings: targetVictims,
	}, framework.NewResult(framework.Success)
}
