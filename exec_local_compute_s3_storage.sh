# For testing only !

# Define authentication through env variables
# Note: Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables outside this script.

#
export S3_BUCKET_NAME=josemrsantos-movie-lens

# Run Spark job to write data to S3
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
             --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
             --conf spark.sql.shuffle.partitions=4 \
             movielens_pipeline.py \
             --input-path s3a://$S3_BUCKET_NAME/dataset/ \
             --output-path s3a://$S3_BUCKET_NAME/output/
