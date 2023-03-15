#! /usr/bin/env bash

OUT_DIR="mixed_ids_result"
NUM_REDUCERS=6

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -D mapreduce.job.name="Streaming mixed ids" \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input "/data/ids" \
    -output ${OUT_DIR} > /dev/null

hdfs dfs -ls ${OUT_DIR} > /dev/null
hdfs dfs -cat ${OUT_DIR}/part-0000* | head -50
