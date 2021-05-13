#!/bin/bash

set -e

if [[ -z "${HOME_DIR}" ]]; then
    echo "Home directory is not defined. Exit"
    exit -1
fi

DATASET_DIR=/workspace/sm-dev/data/dbpedia/instances/wikipedia_links_en
TMP_DIR=$DATASET_DIR/tmp
start=$(date +%s.%N)

# step 1: split the files
# mkdir -p $DATASET_DIR/step_1
# python -m shmr -v -i $DATASET_DIR/step_0/*.bz2 partitions.coalesce --records_per_partition 250000 --outfile $DATASET_DIR/step_1/*.gz

# step 2: convert ttl into json
# mkdir -p $DATASET_DIR/step_2
# ls $DATASET_DIR/step_1/*.gz | parallel --bar -q python -m shmr -i {} -d sm_unk.misc.dbpedia.ttl_loads partition.map --fn sm_unk.misc.dbpedia.triple2json --outfile $DATASET_DIR/step_2/*.gz
# echo 'Number of triples:'; python -m shmr -i $DATASET_DIR/step_2/'*'.gz partitions.count --outfile stdout

# step 3: filter irrelevant info
# mkdir -p $DATASET_DIR/step_3
# ls $DATASET_DIR/step_2/*.gz | parallel --bar -q python -m shmr -i {} partition.filter --fn sm_unk.misc.dbpedia.filter_dbpedia_wikipedia_links --outfile $DATASET_DIR/step_3/'*'.gz --delete_on_empty
echo 'Number of triples:'; python -m shmr -i $DATASET_DIR/step_3/'*'.gz partitions.count --outfile stdout

# FINAL: see some lines in the data
# python -m shmr -i $DATASET_DIR/final/'*'.gz partitions.head --n_rows 10

# FINAL: create the final data
# rm -f $DATASET_DIR/final
# ln -s $DATASET_DIR/step_3 $DATASET_DIR/final

# FINAL: test coverage of the data
# rm -rf $TMP_DIR; mkdir -p $TMP_DIR
# ls $DATASET_DIR/final/*.gz | parallel --bar -q python -m shmr -i {} partition.reduce --fn sm_unk.misc.dbpedia.count_link_coverages --outfile $TMP_DIR/*.json
# python -m shmr -i $TMP_DIR/'*'.json partitions.reduce --fn sm_unk.misc.dbpedia.merge_link_coverages --outfile $TMP_DIR/result.txt
# cat $TMP_DIR/result.txt | python -c 'import sys, json; o = json.loads(sys.stdin.read()); print(">>> coverage: %.02f" % (sum(o.values()) * 100 / len(o))); print("\n".join([f"""{k} {k.replace("http://en.wikipedia.org/wiki", "http://dbpedia.org/page")} {v}""" for k, v in o.items() if v == 0]))'

# FINAL: find a link
# ls $DATASET_DIR/final/*.gz | parallel --bar -q python -m shmr -i {} partition.apply --fn sm_unk.misc.dbpedia.test_link

end=$(date +%s.%N)    
runtime=$(python -c "from datetime import timedelta; print(str(timedelta(seconds=${end} - ${start}))[:-3])")
echo ">>> Finish in $runtime"