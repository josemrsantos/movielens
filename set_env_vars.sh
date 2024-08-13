# Define authentication through env variables
# Note: Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables outside this script.
#export AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY_ID # SET OUTSIDE DO NOT SAVE HERE
#export AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_ACCESS_KEY # SET OUTSIDE DO NOT SAVE HERE
# Define the AWS region
export AWS_DEFAULT_REGION=us-east-1
# Define the Bucket name
export S3_BUCKET_NAME=josemrsantos-movie-lens
# Define the EC2 Key Name
export KEY_NAME=movie_lens
#Define the IAM_ROLE_ARN
export IAM_ROLE_ARN=arn:aws:iam::468854024277:role/movie_lens_EMR_serverless

# More information on these definitions can be found in the README.md file