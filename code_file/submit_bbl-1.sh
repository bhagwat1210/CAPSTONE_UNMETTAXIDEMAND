#!/bin/bash
PYTHON_FILE=$1
INPUT_FILE=$2
OUTPUT=$3
GEOJSON=$4
LOCAL_OUTPUT=$5
NUM_EXECS=512
EXEC_MEM=5g

HD=hadoop
SP=spark-submit

$HD fs -rm -r -skipTrash $OUTPUT
$SP --num-executors $NUM_EXECS --executor-memory $EXEC_MEM --files $GEOJSON $PYTHON_FILE $INPUT_FILE $GEOJSON $OUTPUT
rm -f $LOCAL_OUTPUT
$HD fs -cat $OUTPUT/part* > $LOCAL_OUTPUT

#Running Script
# ./submit_bbl.sh tpep2015_triprecord_bbl_mapping.py hdfs:///gws/ /user/bsb342/bbl_output MNMapPluto.geojson ./textfile.txt
