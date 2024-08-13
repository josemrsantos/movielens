# exec_EMR_compute_s3_storage.sh

# Note: Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables outside this script.
export CLUSTER_ID=your-cluster-id # You get this ID when you create the cluster
export STEP_ID=your-step-id # You get this ID when you add the step

# Create the default EMR roles if they do not exist
if ! aws iam get-role --role-name EMR_EC2_DefaultRole &>/dev/null; then
  echo "Need to create the ERM default roles"
  aws emr create-default-roles
fi

# Copy script to S3 bucket
aws s3 cp movielens_pipeline.py s3://$S3_BUCKET_NAME/scripts/
echo "File copied. Starting the creation of the EMR cluster."

# Create an EMR cluster with spot instances and CloudWatch logging
CLUSTER_ID=$(aws emr create-cluster --name "movie-lens-cluster" \
  --release-label emr-7.2.0 \
  --applications Name=Spark \
  --ec2-attributes KeyName=$KEY_NAME \
  --use-default-roles \
  --log-uri s3://$S3_BUCKET_NAME/logs/ \
  --instance-fleets file://<(cat <<EOF
[
  {
    "InstanceFleetType": "MASTER",
    "TargetSpotCapacity": 1,
    "InstanceTypeConfigs": [
      {
        "InstanceType": "m5.xlarge",
        "WeightedCapacity": 1
      }
    ]
  },
  {
    "InstanceFleetType": "CORE",
    "TargetSpotCapacity": 2,
    "InstanceTypeConfigs": [
      {
        "InstanceType": "m5.xlarge",
        "WeightedCapacity": 1,
        "BidPrice": "0.13"
      }
    ]
  }
]
EOF
) \
  --query 'ClusterId' --output text)


echo "Cluster created with the id $CLUSTER_ID. Now submitting the job to Spark."

# Submit Spark Job to EMR Cluster
STEP_ID=$(aws emr add-steps --cluster-id $CLUSTER_ID \
                  --steps Type=Spark,Name="movie-lens-job",ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--conf,spark.sql.shuffle.partitions=2,s3://josemrsantos-movie-lens/scripts/movielens_pipeline.py,--input-path,s3a://josemrsantos-movie-lens/dataset/,--output-path,s3a://josemrsantos-movie-lens/output/] \
                  --query 'StepIds[0]' --output text)
echo "Job submitted with step_id = $STEP_ID. Now waiting for the job to complete"

# Wait for the job to complete
aws emr wait step-complete --cluster-id $CLUSTER_ID --step-id $STEP_ID
echo "Job has completed. Will now terminate the Cluster $CLUSTER_ID"

# Terminate the EMR cluster
aws emr terminate-clusters --cluster-ids $CLUSTER_ID
echo "Cluster terminated"

# Fetch the logs and output them here (useful for local test and for looking at Docker output)
export PATH_STDOUT="s3://$S3_BUCKET_NAME/"$(aws s3 ls s3://$S3_BUCKET_NAME/logs --recursive | grep $CLUSTER_ID | grep 0001/stdout | cut -c 32-)
aws s3 cp $PATH_STDOUT ./
gunzip -f stdout.gz
cat stdout