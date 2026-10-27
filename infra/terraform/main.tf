# Terraform Infrastructure Definition for Real-Time Event & GenAI Knowledge Platform
# Deploys Amazon EKS, Amazon MSK (Kafka), and S3 Lakehouse Buckets

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-1"
}

variable "cluster_name" {
  default = "realtime-genai-eks-cluster"
}

# 1. VPC & Networking
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.1.0"

  name = "realtime-event-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true
}

# 2. S3 Lakehouse Bucket for Apache Iceberg
resource "aws_s3_bucket" "iceberg_lakehouse" {
  bucket = "realtime-event-iceberg-lakehouse-${var.aws_region}"
}

resource "aws_s3_bucket_versioning" "iceberg_versioning" {
  bucket = aws_s3_bucket.iceberg_lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# 3. Amazon EKS Cluster (Kubernetes for Flink & GenAI)
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "19.21.0"

  cluster_name    = var.cluster_name
  cluster_version = "1.28"

  cluster_endpoint_public_access = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    streaming_nodes = {
      name           = "flink-streaming-workers"
      instance_types = ["m6i.2xlarge"]
      min_size       = 3
      max_size       = 10
      desired_size   = 3
    }
    genai_gpu_nodes = {
      name           = "ray-genai-workers"
      instance_types = ["g5.2xlarge"] # GPU node for vLLM & embeddings
      min_size       = 1
      max_size       = 4
      desired_size   = 2
    }
  }
}

# 4. Amazon MSK (Managed Apache Kafka)
resource "aws_msk_cluster" "kafka_stream" {
  cluster_name           = "realtime-clickstream-cluster"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type = "kafka.m5.xlarge"
    client_subnets = module.vpc.private_subnets
    storage_info {
      ebs_storage_info {
        volume_size = 1000 # 1TB per broker
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
    }
  }
}

output "eks_cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "s3_lakehouse_bucket" {
  value = aws_s3_bucket.iceberg_lakehouse.bucket
}

output "msk_bootstrap_brokers" {
  value = aws_msk_cluster.kafka_stream.bootstrap_brokers
}
