docker run -it --rm \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -v $(pwd)/output:/app/output \
  movie-lens bash ./exec_local_compute_local_storage.sh
