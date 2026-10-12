###############################################################################
# Neuron DRA driver (Dynamic Resource Allocation for Trainium/Inferentia)
#
# The core of the architecture: it advertises Neuron devices as rich
# ResourceSlice objects (DeviceClass "neuron.aws.com") instead of opaque
# integer counts, so workloads select hardware declaratively. Requires
# Kubernetes 1.34+.
#
# NOTE: devicePlugin.enabled=false is critical. The DRA driver and the Neuron
# device plugin MUST NOT run on the same node (silent oversubscription).
###############################################################################
resource "helm_release" "neuron_dra" {
  name       = "neuron-helm-chart"
  repository = "oci://public.ecr.aws/neuron"
  chart      = "neuron-helm-chart"
  version    = "1.10.0" # pinned for reproducibility
  # The chart templates its own "neuron-dra-driver" Namespace and hardcodes the
  # DRA DaemonSet/ServiceAccount into it, so helm must NOT pre-create it
  # (that fails with 'namespaces "neuron-dra-driver" already exists').
  # Only the helm release record lives in kube-system.
  namespace        = "kube-system"
  create_namespace = false

  set {
    name  = "devicePlugin.enabled"
    value = "false"
  }
  set {
    name  = "npd.enabled"
    value = "false"
  }
  set {
    name  = "draDriver.enabled"
    value = "true"
  }

  depends_on = [module.eks]
}

###############################################################################
# KubeRay operator — manages Ray clusters / RayService (vLLM runs on top).
#
# Must be >= 1.4: older CRDs/operator only know the pre-1.31 PodSpec
# `resourceClaims[].source.resourceClaimTemplateName` shape, so the DRA claim
# on the Ray worker pod gets pruned and the pod is rejected on k8s 1.34.
# NOTE: helm does not upgrade CRDs on `helm upgrade`. On an existing cluster
# that was installed with an older chart, apply the new CRDs first:
#   helm pull kuberay/kuberay-operator --version 1.7.1 --untar
#   kubectl apply --server-side --force-conflicts -f kuberay-operator/crds/
###############################################################################
resource "helm_release" "kuberay_operator" {
  name             = "kuberay-operator"
  repository       = "https://ray-project.github.io/kuberay-helm/"
  chart            = "kuberay-operator"
  version          = "1.7.1"
  namespace        = "kuberay-operator"
  create_namespace = true

  depends_on = [module.eks]
}
