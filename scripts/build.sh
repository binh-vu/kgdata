set -e

export RUST_LOG=info

# python -m kgdata.dbpedia.datasets -d generic_extractor_dump
# python -m kgdata.dbpedia.datasets -d mapping_extractor_dump
# python -m kgdata.dbpedia.datasets -d entities
# python -m kgdata.dbpedia.datasets -d ontology_dump
# python -m kgdata.dbpedia.datasets -d classes
# python -m kgdata.dbpedia.datasets -d properties
# python -m kgdata.dbpedia.datasets -d entity_all_types
# python -m kgdata.dbpedia.datasets -d entity_degrees
# python -m kgdata.dbpedia.datasets -d entity_types_and_degrees
python -m kgdata.dbpedia.datasets -d redirection_dump


# python -m kgdata.wikidata entities -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
# python -m kgdata.wikidata entity_labels -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
# python -m kgdata.wikidata classes -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
# python -m kgdata.wikidata properties -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
# python -m kgdata.wikidata wp2wd -d data/wikidata/20211213 -o data/databases/20211213_v2 -c -l en
# python -m kgdata.wikidata entity_redirections -d data/wikidata/20211213 -o data/databases/20211213_v2 -c
