#!/bin/bash

set -e

if [[ -z "${HOME_DIR}" ]]; then
    echo "Home directory is not defined. Exit"
    exit -1
fi

DATASET_DIR=/workspace/sm-dev/data/dbpedia/instances/redirects_en
TMP_DIR=$DATASET_DIR/tmp

STEP_1=0
STEP_2=0
STEP_FINAL=1

start=$(date +%s.%N)

# step 1: split the files
if [ "$STEP_1" = "1" ]; then
mkdir -p $DATASET_DIR/step_1
python -m shmr -v -i $DATASET_DIR/step_0/'*'.bz2 partitions.coalesce --records_per_partition 250000 --outfile $DATASET_DIR/step_1/'*'.gz
fi

# step 2: convert ttl into json
if [ "$STEP_2" = "1" ]; then
mkdir -p $DATASET_DIR/step_2
ls $DATASET_DIR/step_1/*.gz | parallel --bar -q python -m shmr -i {} -d sm_unk.misc.dbpedia.ttl_loads partition.map --fn sm_unk.misc.dbpedia.triple2json --outfile $DATASET_DIR/step_2/'*'.gz
echo 'Number of triples:'; python -m shmr -i $DATASET_DIR/step_2/'*'.gz partitions.count --outfile stdout

# post step 2: get list of predicates
rm -rf $TMP_DIR; mkdir -p $TMP_DIR
ls $DATASET_DIR/step_2/*.gz | parallel --bar -q -I? python -m shmr -i ? partition.reduce --fn sm_unk.misc.dbpedia.reduce_predicate_partition --outfile $TMP_DIR/'*'.json --init_val {}
python -m shmr -i $TMP_DIR/'*'.json partitions.reduce --fn sm_unk.misc.dbpedia.reduce_predicate_partitions --outfile $TMP_DIR/result.txt --init_val {}
cat $TMP_DIR/result.txt; rm -rf $TMP_DIR
fi

# FINAL: create the final data
if [ "$STEP_FINAL" = "1" ]; then
rm -f $DATASET_DIR/final
ln -s $DATASET_DIR/step_2 $DATASET_DIR/final

# FINAL: see some lines in the data
python -m shmr -i $DATASET_DIR/final/'*'.gz partitions.head --n_rows 10
fi

# ls $DATASET_DIR/final/*.gz | parallel --bar -q python -m shmr -i {} partition.apply --fn sm_unk.misc.dbpedia.test_link

end=$(date +%s.%N)    
runtime=$(python -c "from datetime import timedelta; print(str(timedelta(seconds=${end} - ${start}))[:-3])")
echo ">>> Finish in $runtime"