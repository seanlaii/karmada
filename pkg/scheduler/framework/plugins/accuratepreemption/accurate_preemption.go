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

package accuratepreemption

import (
	"context"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	estimatorclient "github.com/karmada-io/karmada/pkg/estimator/client"
	"github.com/karmada-io/karmada/pkg/scheduler/framework"
	"k8s.io/klog/v2"
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
	var targetVictims []workv1alpha2.ObjectReference
	klog.Infof("PostFilter bindingSpec: %v", bindingSpec)
	klog.Infof("PostFilter clusters: %v", clusters)
	estimators := estimatorclient.GetReplicaEstimators()
	for name, estimator := range estimators {
		if name != "scheduler-estimator" {
			klog.Infof("only use scheduler-estimator")
			continue
		}
		victimCandidatesList, err := estimator.GetVictimResourceBindings(ctx, clusters, bindingSpec.ReplicaRequirements, bindingSpec.Replicas, bindingSpec.Resource)
		klog.Infof("victim Candidate length: %v, data: %v", len(victimCandidatesList), victimCandidatesList)
		if err != nil {
			klog.Errorf("Get Victim Resource Bindings replicas error: %v, estimator name: %v", err, name)
			continue
		}
		for _, victimCandidates := range victimCandidatesList {
			if len(targetVictims) == 0 || len(victimCandidates) < len(targetVictims) {
				targetVictims = victimCandidates
			}
		}
	}
	klog.Infof("targetVictims: %v", targetVictims)
	return &framework.PreemptionTargets{
		TargetBindings: targetVictims,
	}, framework.NewResult(framework.Success)
}
