#!/bin/bash

set -e

if [[ -z "${HOME_DIR}" ]]; then
    echo "Home directory is not defined. Exit"
    exit -1
fi

DATASET_DIR=/workspace/sm-dev/data/dbpedia/tables_en

STEP_1=0
STEP_2=0
# DERIV_STEP_3=0
# DERIV_STEP_4=1
# STEP_FINAL=0

TMP_DIR=$DATASET_DIR/tmp
start=$(date +%s.%N)

# step 1: convert raw tables to dbpedia tables
if [ "$STEP_1" = "1" ]; then
echo ">>> step 1 convert raw tables from dbpedia ttl to json"
python -m sm_unk.func_cmd_runner \
    -f kg_data.dbpedia.table_extraction.convert_raw_tables_step0 \
    -a infile $DATASET_DIR/step_0/raw-tables_lang=en.ttl.bz2 \
    -a outdir $DATASET_DIR/step_1 \
    -a pages_per_file 50000
fi

# step 2: convert dbpedia tables to my raw tables
if [ "$STEP_2" = "1" ]; then
echo ">>> step 2 normalize the json table to expand the span issue"
ls $DATASET_DIR/step_1/*.gz | xargs -n 1 -P 8 python -m sm_unk.func_cmd_runner \
    -f kg_data.dbpedia.table_extraction.convert_raw_tables_step1 \
    -a report true \
    -a outdir $DATASET_DIR/step_2 \
    -a infile
fi

# # derivative step 3: filter to keep relational tables only
# if [ "$DERIV_STEP_3" = "1" ]; then
# echo ">>> DERIV_STEP_3"
# fi

# # derivative step 4: extract wikipedia links in tables
# if [ "$DERIV_STEP_4" = "1" ]; then
# echo ">>> DERIV_STEP_4"
# rm -rf $TMP_DIR; rm -rf $DATASET_DIR/step_4_wikilinks
# # extract links
# EXEC_DIR=$DATASET_DIR/tmp shmr flat_map -i $DATASET_DIR/step_3_relational_tables/*.gz -o $TMP_DIR/s0/'*'.gz \
#     --fn sm_unk.misc.dbpedia.extract_wiki_links_from_table --auto_mkdir

# # ls $DATASET_DIR/step_3_relational_tables/*.gz | parallel --bar -q -I? python -m shmr -i ? \
# #     -s shmr.str_dumps partition.flat_map \
# #     --fn sm_unk.misc.dbpedia.extract_wiki_links_from_table --outfile $TMP_DIR/s0/'*'.gz --auto_mkdir

# # filter duplicated
# # ls $TMP_DIR/s0/*.gz | parallel --bar -q -I? python -m shmr -i ? -d shmr.str_loads -s shmr.str_dumps partition.split_by_key \
# #     --fn shmr.str2hashnumber --outfile $TMP_DIR/s1/{stem}/{auto}.gz --num_partitions 128 --auto_mkdir
# # seq -f %05g 0 127 | parallel --bar -q -I? python -m shmr \
# #     -i $TMP_DIR/s1/'*'/?.gz partitions.concat --outfile $TMP_DIR/s2/?.gz --auto_mkdir

# # ls $TMP_DIR/s2/*.gz | parallel --bar -q -I? python -m shmr -i ? -d shmr.str_loads -s shmr.str_dumps partition.distinct \
# #     --fn shmr.identity --outfile $DATASET_DIR/step_4_wikilinks/'*'.gz --auto_mkdir

# # # show the final result
# # python -m shmr -i $DATASET_DIR/step_4_wikilinks/'*'.gz partitions.head --n 10
# # python -m shmr -i $DATASET_DIR/step_4_wikilinks/'*'.gz partitions.count --outfile stdout
# # rm -rf $TMP_DIR
# fi

end=$(date +%s.%N)    
runtime=$(python -c "from datetime import timedelta; print(str(timedelta(seconds=${end} - ${start}))[:-3])")
echo ">>> Finish in $runtime"