# Local compute and local storage - useful for script debugging
spark-submit \
    --master local[*] \
    movielens_pipeline.py \
    --input-path ./dataset \
    --output-path ./output
