set -e

kgdata wikidata -b entities -d data/wikidata/20211213 -o data/wikidata/20211213/databases -c -l en
kgdata wikidata -b classes -d data/wikidata/20211213 -o data/wikidata/20211213/databases -c -l en
kgdata wikidata -b properties -d data/wikidata/20211213 -o data/wikidata/20211213/databases -c -l en
kgdata wikidata -b wp2wd -d data/wikidata/20211213 -o data/wikidata/20211213/databases -c -l en
kgdata wikidata -b entity_labels -d data/wikidata/20211213 -o data/wikidata/20211213/databases -c -l en
kgdata wikidata -b entity_redirections -d data/wikidata/20211220 -o data/wikidata/20211213/databases -c
