/*
Copyright 2020 The Karmada Authors.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:storageversion
// +kubebuilder:resource:scope=Cluster
// +kubebuilder:resource:path=federatedpriorityclasses,scope="Cluster",shortName=fpc,categories={karmada-io}
// +kubebuilder:printcolumn:name="Value",JSONPath=".value",type=integer,description="Value of federatedPriorityClass's Priority"

// FederatedPriorityClass is the Schema for the federatedPriorityClass API
type FederatedPriorityClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// value represents the integer value of this federatedPriorityClass. This is the actual priority that workloads
	// receive when jobs have the name of this class in their federatedPriorityClass label.
	// Changing the value of federatedPriorityClass doesn't affect the priority of workloads that were already created.
	Value int32 `json:"value"`

	// description is an arbitrary string that usually provides guidelines on
	// when this federatedPriorityClass should be used.
	// +optional
	Description string `json:"description,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// FederatedPriorityClassList contains a list of FederatedPriorityClass
type FederatedPriorityClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	// Items is the list of FederatedPriorityClass.
	Items []FederatedPriorityClass `json:"items"`
}
