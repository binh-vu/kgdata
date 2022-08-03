set -e

export RUST_LOG=info

python -m kgdata.wikidata entities -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
python -m kgdata.wikidata entity_labels -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
python -m kgdata.wikidata classes -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
python -m kgdata.wikidata properties -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
python -m kgdata.wikidata wp2wd -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
python -m kgdata.wikidata entity_redirections -d data/wikidata/20211213 -o data/databases/20211213_v2 -c
