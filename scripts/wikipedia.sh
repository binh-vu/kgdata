#!/bin/bash

set -e

# setting python path
export PYTHONPATH=/home/rook/workspace/sm-dev/sm-unknown:$PYTHONPATH
if [[ -z "${HOME_DIR}" ]]; then
    echo "Home directory is not defined. Exit"
    exit -1
fi

SPLIT_FILE=0
EXTRACT_TABLE=0
EXTRACT_REDIRECT=0
GROUP_ARTICLES=0

DATASET_DIR=/workspace/sm-dev/data/wikipedia/pages_articles_en

# 1. splitting the big pages_articles_en to smaller files
if [ "$SPLIT_FILE" = "1" ]; then
pages_per_file=50000
python -c "from sm_unk.misc import wiki; wiki.split_page_articles(\"$DATASET_DIR/step_0/enwiki-20200201-pages-articles.xml.bz2\", \"$DATASET_DIR/step_1\", $pages_per_file)"
fi

# 2. extract tables from these files. DEPRECATED! use DBPedia Raw tables instead
if [ "$EXTRACT_TABLE" = "1" ]; then
in_dir=/home/rook/workspace/sm-dev/data/wikipedia/pages_articles_en
out_dir=/home/rook/workspace/sm-dev/data/wikipedia/pages_articles_en_tables
script="/tmp/script_$(uuidgen).py"
echo "import sys, sm_unk.misc as M" > $script
echo "from pathlib import Path" >> $script
echo "outdir, infile = sys.argv[1:]" >> $script
echo "M.wiki.extract_raw_tables(infile, str(Path(outdir) / Path(infile).name), report=True)" >> $script

ls $in_dir/*.gz | xargs -n 1 -P 8 python $script $out_dir
rm $script
fi

# 3. extract redirects information
if [ "$EXTRACT_REDIRECT" = "1" ]; then
mkdir -p $DATASET_DIR/identifications

script="/tmp/script_$(uuidgen).py"
echo "import sys" > $script
echo "from pathlib import Path" >> $script
echo "from kg_data.wikipedia import extract_redirect_links" >> $script
echo "outdir, infile = sys.argv[1:]" >> $script
echo "extract_page_identifications(infile, str(Path(outdir) / Path(infile).name))" >> $script
ls $DATASET_DIR/step_1/*.gz | parallel --bar -q -I? python $script $DATASET_DIR/identifications ?
fi

# 4. group articles
if [ "$GROUP_ARTICLES" = "1" ]; then
python -m sm_unk.func_cmd_runner -f kg_data.wikipedia.group_pages --infile $DATASET_DIR/identifications/'*'.gz --outdir $DATASET_DIR/titles_groups
fi