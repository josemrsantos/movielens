# MovieLens Data Processing Pipeline

## Project Summary

The MovieLens Data Processing Pipeline is designed to process movie and rating data from the MovieLens dataset. The
pipeline performs the following tasks:

1. Reads in `movies.dat` and `ratings.dat` files into Spark DataFrames.
2. Creates a new DataFrame that includes the movies data along with three new columns: max_rating, min_rating, and
   avg_rating for each movie based on the ratings data.

3. Creates another DataFrame that contains each user's (`userId` in the ratings data) top 3 movies based on their
   ratings.
4. Writes out the original and new DataFrames in an efficient format (Parquet).

As we only have actions in the last step, that is when the entire execution is triggered.

## Prerequisites

- Python 3.x
- Apache Spark
- PySpark
- pytest (for running tests)

## Project Structure

```
movie_lens/
├── dataset/
│   ├── movies.dat
│   └── ratings.dat
├── tests/
│   ├── test_data_processing.py
│   ├── test_exceptions_handling.py
│   ├── test_output_results.py
│   ├── test_data_loading.py
│   ├── test_spark_session.py
│   ├── test_movie_statistics.py
│   ├── test_top_movies_per_user.py
│   └── test_output_format.py
├── check_local_results.py
├── create_role_emr_serverless.sh
├── exec_EMR_compute_s3_storage.sh
├── exec_local_compute_local_storage.sh
├── exec_local_compute_s3_storage.sh
├── movielens_pipeline.py
├── requirements.txt
├──run_docker_EMR_compute_s3_storage.sh
├──run_docker_local_compute_local_storage.sh
├──run_docker_local_compute_s3_storage.sh
├── set_env_vars.sh
└── README.md
```

## Getting Started

### Installation

1. Clone the repository:

```sh
git clone https://github.com/yourusername/movie_lens.git
cd movie_lens
```

2. Create and activate a virtual environment:

```sh
python -m venv venv
source venv/bin/activate
```

3. Install the required dependencies:

```sh
pip install -r requirements.txt
```

### Building the Docker Image

A Docker image for the MovieLens Data Processing Pipeline can be created with:
```sh
docker build -t movielens_pipeline .
```

### Running the Pipeline

Running this pipeline can be as simple as running the exec_*.sh shell scripts described bellow.  
A few scripts however will need some configuration done and resources manually created.

It is also possible to run the same scripts from inside Docker containers that can themselves
run locally or on AWS.

#### Configure

Apart from using local compute and local storage, you will need to alter the script `set_env_vars.sh`.  
This script sets important env vars that serve as configuration:

- **AWS_DEFAULT_REGION**: Your choice for the region you want to use (I have used `us-east-1`).
- **S3_BUCKET_NAME**: The name (not ARN) of the bucket that you need to create first.
- **KEY_NAME**: Specifies the name of the SSH key pair used to securely connect to and manage the EC2 instances in the
  EMR cluster. If not already created, you can create an SSH key pair using the AWS Management Console -> EC2 -> Key
  pairs -> Add.
- **IAM_ROLE_ARN**: Specifies the Amazon Resource Name (ARN) of the IAM role that the EMR cluster will assume. This role
  should have the necessary permissions to access the required AWS resources. If not already created, you can create an
  IAM role using the AWS Management Console -> IAM -> Roles -> Create role, and then attach the necessary policies. For
  testing purposes, here are some policies that you might consider adding to this role:
    - AmazonS3FullAccess
    - AmazonEMRFullAccessPolicy_v2
    - CloudWatchLogsFullAccess
    - IAMFullAccess
    - EC2FullAccess
    - AmazonVPCFullAccess

#### Execute

You can run the pipeline using any of the provided exec_ shell commands. Each script is designed to execute the pipeline
in different environments:

- **exec_local_compute_local_storage.sh**: Executes the pipeline using local compute and local storage.
- **exec_local_compute_s3_storage.sh**: Executes the pipeline using local compute and S3 storage.
- **exec_EMR_compute_s3_storage.sh**: Executes the pipeline using EMR compute and S3 storage.

For the last two, remember to change and run the script `set_env_vars.sh` first with `source set_env_vars.sh`.

#### Examples

Run in local PySpark and output to a local directory called `output`:

```sh
./exec_local_compute_local_storage.sh
```

Run in local PySpark and output to an S3 Bucket (configured in `set_env_vars.sh`):

```sh
source set_env_vars.sh
./exec_local_compute_s3_storage.sh
```

Run in an AWS EMR Spark cluster and output to an S3 Bucket (all configured in `set_env_vars.sh`):

```sh
./exec_EMR_compute_S3_storage.sh
```

### Running the Docker Scripts locally

Executes the pipeline using local compute and local storage.
```sh
docker build -t movielens_pipeline .
docker run -v $(pwd):/app movielens_pipeline
```

Executes the pipeline using local compute and S3 storage.
```sh
source set_env_vars.sh
docker run -v $(pwd):/app movielens_pipeline
```

Executes the pipeline using EMR compute and S3 storage.
```sh
source set_env_vars.sh
./exec_EMR_compute_s3_storage.sh
```


### Command-Line Arguments

If you prefer to run the pipeline directly using spark-submit, you can use the following command-line arguments:

- `--input_path`: The path to the directory containing the `movies.dat` and `ratings.dat` files.
- `--output_path`: The path to the directory where the output DataFrames will be saved.

#### Example

```sh
spark-submit movielens_pipeline.py --input_path dataset/ --output_path output/
```

### Note on Lazy Evaluation

Due to Spark's lazy evaluation nature, transformations on DataFrames are not executed immediately. Instead, they are
recorded as a lineage of transformations to be applied when an action is called. In this pipeline, the actual
computation is triggered by the action within the `output_result` function:

```python
output_result(ratings_df, movies_df, movies_with_stats_df, top_movies_per_user_df, output_base_path)
```

Everything before this call is executed using lazy evaluation, and the transformations are only applied when this action
is called.

## Testing

To run the tests, use the following command:

```sh
pytest tests/
```

It is recommended to use a virtual environment and install the dependencies there.

### Other Scripts

- `check_local_results.py`: Script to check the results of the local computation.
- `create_role_emr_serverless.sh`: Shell script to create an EMR serverless role.

## Costs considerations

Having run several times both the scripts that output to S3 and the script that starts an EMR cluster, runs the script,
and then destroys the cluster, on one day I can say that I have spent $0.37.

Using only spot instances for the EMR cluster was decisive in keeping the cost down. Using that approach might be (or
not) a good option for production code, depending on several other design options of the data pipeline (e.g., if a
transformation fails, can it be repeated? Is this transformation idempotent?).

## Versioning

This project uses semantic versioning. The current version is stored in the `VERSION` file.

### Updating the Version

1. Update the `VERSION` file with the new version number.
2. Update the `CHANGES.md` file with the changes for the new version.
3. Commit the changes:
   ```sh
   git add VERSION CHANGES.md
   git commit -m "Bump version to x.y.z``
4. Tag the new version on github:
   ```sh
   git tag -a vX.Y.Z -m "Release version X.Y.Z"
   git push origin vX.Y.Z```
5. Push the changes to the repository:
   ```sh
    git push origin main
   ```


## Changelog
See the [CHANGELOG](CHANGES.md) for details on changes and updates.