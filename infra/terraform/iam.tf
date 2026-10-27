# IAM Roles and Policies for EKS Service Accounts (IRSA)
# Ensures Flink and GenAI services have least-privilege access to S3 Iceberg buckets

# 1. IAM Policy for Apache Iceberg S3 Read/Write
resource "aws_iam_policy" "iceberg_s3_access_policy" {
  name        = "IcebergS3LakehouseAccessPolicy"
  description = "Allows Flink and Spark streaming jobs to read and write Parquet data into Iceberg S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.iceberg_lakehouse.arn,
          "${aws_s3_bucket.iceberg_lakehouse.arn}/*"
        ]
      }
    ]
  })
}

# 2. IAM Role for Flink Streaming Service Account
resource "aws_iam_role" "flink_service_account_role" {
  name = "flink-streaming-lakehouse-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = module.eks.oidc_provider_arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "${module.eks.oidc_provider}:sub" : "system:serviceaccount:streaming:flink-service-account"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "flink_attach" {
  role       = aws_iam_role.flink_service_account_role.name
  policy_arn = aws_iam_policy.iceberg_s3_access_policy.arn
}

# 3. Output IAM Role ARN for Kubernetes ServiceAccount annotations
output "flink_iam_role_arn" {
  description = "ARN to annotate into Kubernetes ServiceAccount for Flink"
  value       = aws_iam_role.flink_service_account_role.arn
}
