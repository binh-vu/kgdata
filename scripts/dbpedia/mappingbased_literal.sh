#!/bin/bash

set -e

if [[ -z "${HOME_DIR}" ]]; then
    echo "Home directory is not defined. Exit"
    exit -1
fi

DATASET_DIR=/workspace/sm-dev/data/dbpedia/instances/mappingbased_literals_en
TMP_DIR=$DATASET_DIR/tmp
start=$(date +%s.%N)

# step 1: split the files
mkdir -p $DATASET_DIR/step_1
python -m shmr -v -i $DATASET_DIR/step_0/'*'.bz2 partitions.coalesce --records_per_partition 250000 --outfile $DATASET_DIR/step_1/'*'.gz

# step 2: convert ttl into json
mkdir -p $DATASET_DIR/step_2
ls $DATASET_DIR/step_1/*.gz | parallel --bar -q python -m shmr -i {} -d sm_unk.misc.dbpedia.ttl_loads partition.map --fn sm_unk.misc.dbpedia.triple2json --outfile $DATASET_DIR/step_2/'*'.gz
echo 'Number of triples:'; python -m shmr -i $DATASET_DIR/step_2/'*'.gz partitions.count --outfile stdout

# FINAL: create the final data
rm -f $DATASET_DIR/final
ln -s $DATASET_DIR/step_2 $DATASET_DIR/final

# FINAL: see some lines in the data
python -m shmr -i $DATASET_DIR/final/'*'.gz partitions.head --n_rows 10

end=$(date +%s.%N)    
runtime=$(python -c "from datetime import timedelta; print(str(timedelta(seconds=${end} - ${start}))[:-3])")
echo ">>> Finish in $runtime"