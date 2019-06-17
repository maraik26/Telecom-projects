#!/bin/bash

export SPARK_MAJOR_VERSION=2
export PYSPARK_DRIVER_PYTHON=/opt/anaconda3/bin/python
export PYSPARK_PYTHON=/opt/anaconda3/bin/python

spark-submit join_all.py