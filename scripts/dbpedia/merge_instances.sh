#!/bin/bash

set -e

if [[ -z "${HOME_DIR}" ]]; then
    echo "Home directory is not defined. Exit"
    exit -1
fi

DATASET_DIR=/workspace/sm-dev/data/dbpedia/instances/merged_instances_en
NUM_PARTITIONS=64
PROP_BASEDIR=/workspace/sm-dev/data/dbpedia/instances
PROP_NAMES=(instance_types_specific_en wikipedia_links_en mappingbased_objects_en mappingbased_literals_en)
# PROP_NAMES=(instance_types_specific_en)

STEP_1=0
STEP_2=0
STEP_3=0
STEP_FINAL=0

TMP_DIR=$DATASET_DIR/tmp
start=$(date +%s.%N)

# step 1: group by instance URIs
if [ "$STEP_1" = "1" ]; then
echo ">>> step 1"
mkdir -p $DATASET_DIR/step_1

for PROP_NAME in ${PROP_NAMES[@]}; do
    mkdir -p $DATASET_DIR/step_1/$PROP_NAME

    # step 1.1: parallel group by
    rm -rf $TMP_DIR; mkdir $TMP_DIR
    ls $PROP_BASEDIR/$PROP_NAME/final/*.gz | parallel --bar -q -I? python -m shmr -i ? partition.split_by_key \
        --num_partitions $NUM_PARTITIONS --fn sm_unk.misc.dbpedia.get_bucket_key_of_subject \
        --outfile $TMP_DIR/{stem}/{auto}.gz --auto_mkdir

    # step 1.2: concat
    seq -f %05g 0 $((NUM_PARTITIONS-1)) | parallel --bar -q -I? python -m shmr \
        -i $TMP_DIR/'*'/?.gz partitions.concat --outfile $DATASET_DIR/step_1/$PROP_NAME/?.gz
done
fi

# step 2: concatenate props together
if [ "$STEP_2" = "1" ]; then
echo ">>> step 2"
mkdir -p $DATASET_DIR/step_2
seq -f %05g 0 $((NUM_PARTITIONS-1)) | parallel --bar -q -I? python -m shmr \
    -i $DATASET_DIR/step_1/'*'/?.gz partitions.concat --outfile $DATASET_DIR/step_2/?.gz
fi

# step 3: merge instances by IDs
if [ "$STEP_3" = "1" ]; then
echo ">>> step 3"
mkdir -p $DATASET_DIR/step_3
ls $DATASET_DIR/step_2/*.gz | parallel --bar -q -I? python -m shmr -i ? partition.reduce_by_key \
    --key_fn sm_unk.misc.dbpedia.get_subject --fn sm_unk.misc.dbpedia.merge_triples --outfile $DATASET_DIR/step_3/'*'.gz
fi

# FINAL: create the final data
if [ "$STEP_FINAL" = "1" ]; then
echo ">>> final step"
rm -f $DATASET_DIR/final
ln -s $DATASET_DIR/step_3 $DATASET_DIR/final

# see some lines in the data
python -m shmr -i $DATASET_DIR/final/'*'.gz partitions.head --n_rows 10
fi

# ls $DATASET_DIR/step_1/wikipedia_links_en/*.gz | parallel --bar -q python -m shmr -i {} partition.apply --fn sm_unk.misc.dbpedia.test_link

end=$(date +%s.%N)    
runtime=$(python -c "from datetime import timedelta; print(str(timedelta(seconds=${end} - ${start}))[:-3])")
echo ">>> Finish in $runtime"