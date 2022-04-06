set -e

kgdata wikidata -d data/wikidata/20211213 -b qnodes -o data/wikidata/20211213/databases -c
kgdata wikidata -d data/wikidata/20211213 -b wdclasses -o data/wikidata/20211213/databases -c
kgdata wikidata -d data/wikidata/20211213 -b wdprops -o data/wikidata/20211213/databases -c
kgdata wikidata -d data/wikidata/20211213 -b enwiki_links -o data/wikidata/20211213/databases -c
kgdata wikidata -d data/wikidata/20211213 -b qnode_labels -o data/wikidata/20211213/databases -c
kgdata wikidata -d data/wikidata/20211213 -b qnode_identifiers -o data/wikidata/20211213/databases -c
kgdata wikidata -d data/wikidata/20211220 -b qnode_redirections -o data/wikidata/20211213/databases -c