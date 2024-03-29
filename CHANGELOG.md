# CHANGE LOG

## [Unreleased]

### Added

- Add `entity_types` db

## [7.1.0] (2024-03-28)

### Added

- Add option to verify signature of datasets
- Add functions to get `meta_graph_stats`, `easy_tables_metadata`, `easy_table` datasets without dependencies

### Fixed

- Fix domain/ranges of dbpedia property dataset & make them not optional
- Fix deserializing wikidata property in Rust

## [7.0.0] (2024-03-24)

### Added

- Add default classes & properties: `rdf:Resource` and `rdf:type`.
- Supports manual corrections in DBpedia such as `dbo:collectionSize` to `dbo:country`.
- Add `dbpedia.datasets.meta_graph` and `dbpedia.datasets.meta_graph_stats` datasets.

### Changed

- Reuse code: `GenericDB.get_default_props` now calls `ont_property.get_default_props`.
- Drop support for Python 3.9 to use new features in dataclass

### Fixed

- Fix domains/ranges of ontology properties

## [6.5.2] (2024-03-08)

### Fixed

- Update hugedict to `2.12.0` to fix rocksdb loader error (affect `entity_labels` database)

## [6.5.1] (2024-03-08)

### Fixed

- Add classes & properties to `dbpedia.datasets.entities` dataset.
- Fix `OntologyProperty.is_object_property` function (missing `entity` datatype)

### Changed

- Reuse `EntityTypeAndDegree` from wikidata datasets for the `dbpedia.datasets.entity_types_and_degrees` dataset.

## [6.5.0]

### Added

- Add DBpedia `entity_metadata` dataset
- Add `entity_metadata` databases and scripts to build DBpedia databases (`entity_metadata`, `entity_labels`, `entity_redirections`)

### Fixed

- Improve type hints in various places.

### Changed

- Rename DBpedia `redirection_dump` dataset to `entity_redirections`.
- DBpedia `entity_labels` dataset now use the common class `kgdata.models.entity.EntityLabel` to make it similar to Wikidata.
- `kgdata.wikidata.db` cli can print the first key-value in the database when no keys are provided.

## [2.3.3](https://github.com/binh-vu/kgdata/tree/2.3.3) (2022-07-06)

[Full Changelog](https://github.com/binh-vu/kgdata/compare/1.7.1...2.3.3)

### Changed

- Upgrade hugedict from version 1 to version 2 using Rust. Existing RocksDB located in: `<db_folder>/primary` should change to just `<db_folder>`
