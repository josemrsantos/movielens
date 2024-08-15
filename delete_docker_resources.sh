#!/bin/bash

# Set environment variables
export AWS_REGION=${AWS_REGION:-us-east-1}
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export REPOSITORY_NAME=movielens_pipeline

# Get resource IDs
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=movielens_vpc" --query 'Vpcs[0].VpcId' --output text)
SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=movielens_subnet" --query 'Subnets[0].SubnetId' --output text)
SECURITY_GROUP_ID=$(aws ec2 describe-security-groups --filters "Name=tag:Name,Values=movielens_sg" --query 'SecurityGroups[0].GroupId' --output text)
REPO_URI=$(aws ecr describe-repositories --repository-names $REPOSITORY_NAME --query 'repositories[0].repositoryUri' --output text 2>/dev/null)

# Delete ECS task definition
TASK_DEFINITION_ARN=$(aws ecs list-task-definitions --family-prefix movielens_pipeline_task --query 'taskDefinitionArns[0]' --output text)
if [ "$TASK_DEFINITION_ARN" != "None" ]; then
  aws ecs deregister-task-definition --task-definition $TASK_DEFINITION_ARN
fi

# Delete ECR repository
if [ -n "$REPO_URI" ]; then
  aws ecr delete-repository --repository-name $REPOSITORY_NAME --force
fi

# Delete Security Group
if [ "$SECURITY_GROUP_ID" != "None" ]; then
  aws ec2 delete-security-group --group-id $SECURITY_GROUP_ID
fi

# Delete Subnet
if [ "$SUBNET_ID" != "None" ]; then
  aws ec2 delete-subnet --subnet-id $SUBNET_ID
fi

# Delete Internet Gateway
IGW_ID=$(aws ec2 describe-internet-gateways --filters "Name=attachment.vpc-id,Values=$VPC_ID" --query 'InternetGateways[0].InternetGatewayId' --output text)
if [ "$IGW_ID" != "None" ]; then
  aws ec2 detach-internet-gateway --internet-gateway-id $IGW_ID --vpc-id $VPC_ID
  aws ec2 delete-internet-gateway --internet-gateway-id $IGW_ID
fi

# Delete Route Table
RTB_ID=$(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID" --query 'RouteTables[0].RouteTableId' --output text)
if [ "$RTB_ID" != "None" ]; then
  aws ec2 delete-route-table --route-table-id $RTB_ID
fi

# Delete VPC
if [ "$VPC_ID" != "None" ]; then
  aws ec2 delete-vpc --vpc-id $VPC_ID
fi

echo "Resources deleted successfully."