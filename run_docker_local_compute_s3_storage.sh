docker run -it --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
  -e S3_BUCKET_NAME=$S3_BUCKET_NAME \
  -v $(pwd)/output:/app/output \
  movie-lens bash ./exec_local_compute_s3_storage.sh