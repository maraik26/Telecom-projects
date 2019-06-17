#!/bin/bash

export SPARK_MAJOR_VERSION=2
export PYSPARK_DRIVER_PYTHON=/opt/anaconda3/bin/python
export PYSPARK_PYTHON=/opt/anaconda3/bin/python

spark-submit \
--master yarn \
--deploy-mode client \
--queue adhoc \
--num-executors 100 \
--executor-cores 2 \
--driver-memory 6G \
--executor-memory 4G \
--conf spark.app.name=abcdefg \
--conf spark.port.maxRetries=20 \
2_load_to_table.py