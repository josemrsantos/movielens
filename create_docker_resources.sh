#!/bin/bash

# Set environment variables
export AWS_REGION=${AWS_REGION:-us-east-1}
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export REPOSITORY_NAME=movielens_pipeline
#export EXECUTION_ROLE_ARN=<your-execution-role-arn>
export EXECUTION_ROLE_ARN=arn:aws:iam::468854024277:role/movie_lens_execution
#export TASK_ROLE_ARN=<your-task-role-arn>
export TASK_ROLE_ARN=arn:aws:iam::468854024277:role/movie_lens_execution

# Create a VPC, Subnet, and Security Group if they don't exist
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=movielens_vpc" --query 'Vpcs[0].VpcId' --output text)
if [ "$VPC_ID" == "None" ]; then
  VPC_ID=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 --query 'Vpc.VpcId' --output text)
  aws ec2 create-tags --resources $VPC_ID --tags Key=Name,Value=movielens_vpc
  # Create an internet gateway and attach it to the VPC
  IGW_ID=$(aws ec2 create-internet-gateway --query 'InternetGateway.InternetGatewayId' --output text)
  aws ec2 attach-internet-gateway --vpc-id $VPC_ID --internet-gateway-id $IGW_ID
  # Create a route table and a public route
  RTB_ID=$(aws ec2 create-route-table --vpc-id $VPC_ID --query 'RouteTable.RouteTableId' --output text)
  aws ec2 create-route --route-table-id $RTB_ID --destination-cidr-block 0.0.0.0/0 --gateway-id $IGW_ID
fi

SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=movielens_subnet" --query 'Subnets[0].SubnetId' --output text)
if [ "$SUBNET_ID" == "None" ]; then
  SUBNET_ID=$(aws ec2 create-subnet --vpc-id $VPC_ID --cidr-block 10.0.1.0/24 --query 'Subnet.SubnetId' --output text)
  aws ec2 create-tags --resources $SUBNET_ID --tags Key=Name,Value=movielens_subnet
  # Associate the subnet with the route table to make it public
  aws ec2 associate-route-table --subnet-id $SUBNET_ID --route-table-id $RTB_ID
  # Enable auto-assign public IP on the subnet
  aws ec2 modify-subnet-attribute --subnet-id $SUBNET_ID --map-public-ip-on-launch
fi

SECURITY_GROUP_ID=$(aws ec2 describe-security-groups --filters "Name=tag:Name,Values=movielens_sg" --query 'SecurityGroups[0].GroupId' --output text)
if [ "$SECURITY_GROUP_ID" == "None" ]; then
  SECURITY_GROUP_ID=$(aws ec2 create-security-group --group-name movielens_sg --description "Security group for movielens pipeline" --vpc-id $VPC_ID --query 'GroupId' --output text)
  aws ec2 authorize-security-group-ingress --group-id $SECURITY_GROUP_ID --protocol tcp --port 80 --cidr 0.0.0.0/0
  aws ec2 authorize-security-group-ingress --group-id $SECURITY_GROUP_ID --protocol tcp --port 443 --cidr 0.0.0.0/0
  aws ec2 create-tags --resources $SECURITY_GROUP_ID --tags Key=Name,Value=movielens_sg
fi

# Create a repository in ECR if it doesn't exist
REPO_URI=$(aws ecr describe-repositories --repository-names $REPOSITORY_NAME --query 'repositories[0].repositoryUri' --output text 2>/dev/null)
if [ -z "$REPO_URI" ]; then
  aws ecr create-repository --repository-name $REPOSITORY_NAME
  REPO_URI=$ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$REPOSITORY_NAME
fi

# Authenticate Docker to the ECR registry
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Build the Docker image
docker build -t movielens_pipeline:latest .

# Tag your Docker image
docker tag movielens_pipeline:latest $REPO_URI:latest

# Push the Docker image to ECR
docker push $REPO_URI:latest

# Create a new ECS task definition
cat <<EOF > task_definition.json
{
  "family": "movielens_pipeline_task",
  "networkMode": "awsvpc",
  "containerDefinitions": [
    {
      "name": "movielens_pipeline_container",
      "image": "$REPO_URI:latest",
      "memory": 512,
      "cpu": 256,
      "essential": true,
      "entryPoint": ["/bin/sh", "-c"],
      "command": ["./exec_EMR_compute_s3_storage.sh"],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/movielens_pipeline",
          "awslogs-region": "$AWS_REGION",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ],
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "256",
  "memory": "512",
  "executionRoleArn": "$EXECUTION_ROLE_ARN",
  "taskRoleArn": "$TASK_ROLE_ARN"
}
EOF

# Register the task definition
aws ecs register-task-definition --cli-input-json file://task_definition.json