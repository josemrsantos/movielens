#!/bin/bash

# Set environment variables
export AWS_REGION=${AWS_REGION:-us-east-1}
export CLUSTER_NAME=movielens_cluster
export SERVICE_NAME=movielens_service
export TASK_DEFINITION=movielens_pipeline_task

# Fetch the Subnet ID
SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=movielens_subnet" --query 'Subnets[0].SubnetId' --output text)

# Fetch the Security Group ID
SECURITY_GROUP_ID=$(aws ec2 describe-security-groups --filters "Name=tag:Name,Values=movielens_sg" --query 'SecurityGroups[0].GroupId' --output text)

# Create ECS cluster if it doesn't exist
CLUSTER_ARN=$(aws ecs describe-clusters --clusters $CLUSTER_NAME --query 'clusters[0].clusterArn' --output text 2>/dev/null)
if [ "$CLUSTER_ARN" == "None" ]; then
  aws ecs create-cluster --cluster-name $CLUSTER_NAME
fi

# Run the task on ECS Fargate
aws ecs run-task \
  --cluster $CLUSTER_NAME \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNET_ID],securityGroups=[$SECURITY_GROUP_ID],assignPublicIp=ENABLED}" \
  --task-definition $TASK_DEFINITION \
  --overrides '{
    "containerOverrides": [{
      "name": "movielens_pipeline_container",
      "environment": [
        {"name": "AWS_ACCESS_KEY_ID", "value": "'$AWS_ACCESS_KEY_ID'"},
        {"name": "AWS_SECRET_ACCESS_KEY", "value": "'$AWS_SECRET_ACCESS_KEY'"},
        {"name": "AWS_DEFAULT_REGION", "value": "'$AWS_DEFAULT_REGION'"},
        {"name": "S3_BUCKET_NAME", "value": "'$S3_BUCKET_NAME'"},
        {"name": "KEY_NAME", "value": "'$KEY_NAME'"},
        {"name": "IAM_ROLE_ARN", "value": "'$IAM_ROLE_ARN'"}
      ]
    }]
  }'

echo "Task has been submitted to ECS Fargate."