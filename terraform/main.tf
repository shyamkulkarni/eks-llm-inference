data "aws_availability_zones" "available" {
  filter {
    name   = "opt-in-status"
    values = ["opt-in-not-required"]
  }
}

# AZs that actually offer the Neuron instance type (e.g. trn1.2xlarge is only
# in us-east-2c in us-east-2).
data "aws_ec2_instance_type_offerings" "neuron" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.neuron_instance_type]
  }
}

locals {
  all_azs = data.aws_availability_zones.available.names

  # Opt-in-free AZs that offer the Neuron instance type.
  neuron_azs = sort([
    for az in local.all_azs : az
    if contains(data.aws_ec2_instance_type_offerings.neuron.locations, az)
  ])

  # 2 AZs total for EKS, guaranteeing the first one offers the Neuron instance.
  azs = slice(distinct(concat(slice(local.neuron_azs, 0, min(1, length(local.neuron_azs))), local.all_azs)), 0, 2)

  # Private subnets (same order as local.azs) in AZs that offer the Neuron instance.
  neuron_private_subnets = [
    for i, az in local.azs : module.vpc.private_subnets[i]
    if contains(local.neuron_azs, az)
  ]

  tags = var.tags
}

###############################################################################
# VPC
###############################################################################
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.13"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [cidrsubnet(var.vpc_cidr, 4, 0), cidrsubnet(var.vpc_cidr, 4, 1)]
  public_subnets  = [cidrsubnet(var.vpc_cidr, 4, 2), cidrsubnet(var.vpc_cidr, 4, 3)]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  # Tags required by the AWS Load Balancer Controller / EKS subnet discovery.
  public_subnet_tags  = { "kubernetes.io/role/elb" = 1 }
  private_subnet_tags = { "kubernetes.io/role/internal-elb" = 1 }

  tags = local.tags
}

###############################################################################
# EKS cluster
###############################################################################
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.31"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  cluster_endpoint_public_access = true

  # Give the identity running terraform admin on the cluster so kubectl works
  # immediately after apply.
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_addons = {
    coredns                = {}
    kube-proxy             = {}
    vpc-cni                = {}
    eks-pod-identity-agent = {}
  }

  eks_managed_node_groups = {
    # Small on-demand group for system pods (CoreDNS, operators, KubeRay head, etc.)
    system = {
      instance_types = ["m6i.large"]
      min_size       = 1
      max_size       = 2
      desired_size   = 2
      labels         = { role = "system" }

      # The Ray head runs the SAME Neuron DLC image as the workers (so the Ray
      # and Python versions cannot skew). That image is ~18 GB, which does not
      # fit the AMI's default 20 GB root volume — the head pod gets evicted for
      # ephemeral-storage pressure. Give these nodes room for it.
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size = 100
            volume_type = "gp3"
          }
        }
      }
    }

    # The accelerator group: Neuron (Trainium/Inferentia). STATIC capacity.
    # DRA requires static capacity provisioning, so this is a fixed managed node group.
    neuron = {
      ami_type       = "AL2023_x86_64_NEURON" # EKS-optimized Neuron AMI (AL2023)
      instance_types = [var.neuron_instance_type]
      # Only subnets in AZs that offer the Neuron instance type.
      subnet_ids   = local.neuron_private_subnets
      min_size     = var.neuron_desired_size
      max_size     = var.neuron_max_size
      desired_size = var.neuron_desired_size

      labels = {
        role                    = "neuron"
        "aws.amazon.com/neuron" = "true"
      }

      # Taint so only Neuron workloads land here. vLLM pods tolerate this.
      taints = [{
        key    = "aws.amazon.com/neuron"
        value  = "true"
        effect = "NO_SCHEDULE"
      }]

      # Neuron instances need extra EBS for the model + Neuron runtime.
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size = 200
            volume_type = "gp3"
          }
        }
      }
    }
  }

  tags = local.tags
}
