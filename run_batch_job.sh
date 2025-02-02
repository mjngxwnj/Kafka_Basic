#!/bin/bash

echo "Run Spark Batch Job at $(date)"

docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 \
                                        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3 \
                                        spark_script/processing_pipeline/batch_job.py"