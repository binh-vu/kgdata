set -e

export RUST_LOG=info

# for printing configuration
python -m kgdata.config
export LOG_CONFIG=0  # surrpress logging directory configurations
# export BUILD_DATASET_ARGS="--verify-signature"
export BUILD_DATASET_ARGS=""

function dbpedia_dataset {
    echo "Build dbpedia dataset: $1"
    python -m kgdata.dbpedia.datasets $BUILD_DATASET_ARGS -d $1
}

function wikidata_dataset {
    echo "Build wikidata dataset: $1"
    python -m kgdata.wikidata.datasets $BUILD_DATASET_ARGS -d $1 ${@:2}
}

function wikipedia_dataset {
    echo "Build wikipedia dataset: $1"
    python -m kgdata.wikipedia.datasets $BUILD_DATASET_ARGS -d $1 ${@:2}
}

function dbpedia_db {
    echo "Build dbpedia database: $1 storing at $DBP_DBDIR"
    python -m kgdata.dbpedia $1 -c -o $DBP_DBDIR
}

function wikidata_db {
    echo "Build wikidata database: $1 storing at $WD_DBDIR"
    python -m kgdata.wikidata $1 -c -o $WD_DBDIR
}

# ======================================================================
# DBPEDIA Datasets

# dbpedia_dataset generic_extractor_dump
# dbpedia_dataset mapping_extractor_dump
# dbpedia_dataset ontology_dump
# dbpedia_dataset classes
# dbpedia_dataset properties
# dbpedia_dataset entities
# dbpedia_dataset entity_redirections
# dbpedia_dataset entity_labels
# dbpedia_dataset entity_metadata
# dbpedia_dataset entity_all_types
# dbpedia_dataset entity_degrees
# dbpedia_dataset entity_types_and_degrees
# dbpedia_dataset meta_graph
# dbpedia_dataset meta_graph_stats

# ======================================================================
# WIKIDATA Datasets

# # NOTE: uncomment to sign the dump files to avoid re-processing dump file
# # export KGDATA_FORCE_DISABLE_CHECK_SIGNATURE=1
# # python -m kgdata.wikidata.datasets -d entity_dump --sign
# # python -m kgdata.wikidata.datasets -d entity_redirection_dump --sign
# # python -m kgdata.wikidata.datasets -d page_dump --sign

# ----- verified dataset -----
# wikidata_dataset triple_truthy_dump
# wikidata_dataset triple_truthy_dump_derivatives

# # wikidata_dataset page_ids
# # wikidata_dataset page_article_dump

# wikidata_dataset entity_ids
# wikidata_dataset entity_redirections
# wikidata_dataset entities
# wikidata_dataset entity_types
# wikidata_dataset entity_sitelinks

# wikidata_dataset classes
wikidata_dataset acyclic_classes
# wikidata_dataset properties

# wikidata_dataset class_count
# wikidata_dataset property_count
# wikidata_dataset property_domains
# wikidata_dataset property_ranges

# wikidata_dataset entity_metadata
# wikidata_dataset entity_all_types
# wikidata_dataset entity_degrees
# wikidata_dataset entity_labels
# wikidata_dataset entity_types_and_degrees
# wikidata_dataset entity_outlinks
# wikidata_dataset entity_pagerank
# wikidata_dataset entity_wiki_aliases

# wikidata_dataset cross_wiki_mapping
# wikidata_dataset meta_graph
# wikidata_dataset meta_graph_stats

# wikidata_dataset mention_to_entities
# wikidata_dataset norm_mentions

# ----- unverified dataset -----

# ======================================================================
# WIKIPEDIA Datasets

# wikipedia_dataset html_articles
# wikipedia_dataset article_aliases
# wikipedia_dataset article_degrees
# wikipedia_dataset article_links
# wikipedia_dataset article_metadata
# wikipedia_dataset html_tables
# wikipedia_dataset mention_to_articles

# wikipedia_dataset relational_tables
# wikipedia_dataset linked_relational_tables
# wikipedia_dataset easy_tables
# wikipedia_dataset easy_tables_metadata

# ======================================================================
# DBpedia Databases

# dbpedia_db classes
# dbpedia_db properties
# dbpedia_db entities
# dbpedia_db entity_types
# dbpedia_db entity_labels
# dbpedia_db entity_metadata
# dbpedia_db entity_redirections

# ======================================================================
# WIKIDATA Databases

# wikidata_db classes
# wikidata_db properties
# wikidata_db entities
# wikidata_db entity_labels
# wikidata_db entity_metadata
# wikidata_db entity_types
# wikidata_db entity_outlinks
# wikidata_db entity_redirections
# wikidata_db wp2wd
# wikidata_db entity_pagerank
# wikidata_db property_domains
# wikidata_db property_ranges
# wikidata_db ontology_count
# wikidata_db mention_to_entities
# wikidata_db norm_mentions