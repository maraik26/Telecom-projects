#!/bin/bash

export SPARK_MAJOR_VERSION=2
export PYSPARK_DRIVER_PYTHON=/opt/anaconda3/bin/python
export PYSPARK_PYTHON=/opt/anaconda3/bin/python

spark-submit phones_called_num_full.py