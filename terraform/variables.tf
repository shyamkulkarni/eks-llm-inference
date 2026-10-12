variable "region" {
  description = "AWS region. Trainium (trn2/inf2) availability varies by region; us-east-1, us-east-2, us-west-2 are safe bets."
  type        = string
  default     = "us-east-2"
}

variable "cluster_name" {
  description = "Name of the EKS cluster."
  type        = string
  default     = "eks-llm-inference"
}

# DRA (Dynamic Resource Allocation) is GA in Kubernetes 1.34. The Neuron DRA
# driver REQUIRES 1.34+. Do not lower this.
variable "cluster_version" {
  description = "Kubernetes control plane version. Must be 1.34+ for DRA."
  type        = string
  default     = "1.34"
}

# The accelerator node group. Default is trn1.2xlarge (Trainium1, 1 chip =
# 2 NeuronCores) because it is the CHEAPEST DRA-capable Neuron instance.
# inf2 (Inferentia2) is NOT supported: the Neuron DRA driver's node affinity
# excludes all inf*/ml.inf* instance types, so its DaemonSet never runs there.
# In us-east-2, trn1.2xlarge is only offered in us-east-2c; main.tf picks AZs
# that offer this instance type automatically.
# For the full Trainium story (connected device subsets, UltraServer) use trn2.48xlarge.
variable "neuron_instance_type" {
  description = "Neuron (Trainium) instance type for the accelerator node group. Must be DRA-capable (trn*, not inf*)."
  type        = string
  default     = "trn1.2xlarge"
}

variable "neuron_desired_size" {
  description = "Number of accelerator nodes in the static pool (used as min and desired size). Two nodes let KubeRay scale inference across nodes."
  type        = number
  default     = 2
  # NOTE: the EKS module ignores desired_size after the node group exists, so
  # to resize a live group run `aws eks update-nodegroup-config` as well.
}

variable "neuron_max_size" {
  description = "Max accelerator nodes. The managed node group can scale pods' underlying nodes up to this with the cluster-autoscaler, but keep DRA caveats in mind."
  type        = number
  default     = 2
}

variable "vpc_cidr" {
  type    = string
  default = "10.0.0.0/16"
}

variable "tags" {
  description = "Tags applied to every resource the stack creates."
  type        = map(string)
  default = {
    Project = "eks-llm-inference"
    Stack   = "dra-vllm-kuberay"
  }
}
