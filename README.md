# MovieLens Data Processing Pipeline

## Introduction

The MovieLens Data Processing Pipeline is designed to process movie and rating data from the MovieLens dataset. The pipeline performs the following tasks:

1. Reads in movies.dat and ratings.dat files into Spark DataFrames.

2. Creates a new DataFrame that includes the movies data along with three new columns: max_rating, min_rating, and avg_rating for each movie based on the ratings data.

3. Creates another DataFrame that contains each user's (userId in the ratings data) top 3 movies based on their ratings.

4. Writes out the original and new DataFrames in an efficient format (Parquet).

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

### Running the Pipeline

TODO: Finish describing what is needed to run the pipeline

#### Configure
Apart from using local compute and local storage, you will need to alter the script `set_env_vars.sh`.  
This script sets important env vars that serve as configuration:
- **AWS_DEFAULT_REGION**: TODO 
- **S3_BUCKET_NAME**: TODO
- **KEY_NAME**: TODO
- **IAM_ROLE_ARN**: TODO

#### Execute
You can run the pipeline using any of the provided exec_ shell commands. Each script is designed to execute the pipeline in different environments:
 - **exec_local_compute_local_storage.sh**: Executes the pipeline using local compute and local storage.
 - **exec_local_compute_s3_storage.sh**: Executes the pipeline using local compute and S3 storage.
 - **exec_EMR_compute_s3_storage.sh**: Executes the pipeline using EMR compute and S3 storage.

For the last 2, remember to change and run first the script `set_env_vars.sh`.

#### Example
As an example to run in local PySpark and output to a local directory
```sh
./exec_local_compute_local_storage.sh
```

### Command-Line Arguments

If you prefer to run the pipeline directly using spark-submit, you can use the following command-line arguments:

--input_path: The path to the directory containing the movies.dat and ratings.dat files.

--output_path: The path to the directory where the output DataFrames will be saved.

#### Example

```sh
spark-submit movielens_pipeline.py --input_path dataset/ --output_path output/
```

### Note on Lazy Evaluation

Due to Spark's lazy evaluation nature, transformations on DataFrames are not executed immediately. Instead, they are recorded as a lineage of transformations to be applied when an action is called. In this pipeline, the actual computation is triggered by the action within the output_result function:

```python
output_result(ratings_df, movies_df, movies_with_stats_df, top_movies_per_user_df, output_base_path)
```

Everything before this call is executed using lazy evaluation, and the transformations are only applied when this action is called.

## Testing

To run the tests, use the following command:

```sh
pytest tests/
```
It is recommended to use a virtual environment and install the dependencies there.

### Other Scripts

check_local_results.py: Script to check the results of the local computation.

create_role_emr_serverless.sh: Shell script to create an EMR serverless role.

## Costs considerations

Having run several times both the scripts that outputs to S3 and the script
that starts an EMR cluster, runs the script and then destroys the cluster,
on one day I can say that I have spent $0.37.  

Using only spot instances for the EMR cluster was decisive in keeping the cost down.
Using that approach might be (or not) a good option for production code, depending 
on several other design options of the data pipeline (e.g. if a transformation fails, 
can it be repeated ? Is this transformation idempotent ?)