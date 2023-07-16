from __future__ import annotations

import orjson
from rdflib import OWL, RDF

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDataDirCfg
from kgdata.dbpedia.datasets.ontology_dump import Resource, ontology_dump
from kgdata.models.multilingual import MultiLingualString
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.models.wdclass import WDClass


def classes() -> Dataset[WDClass]:
    cfg = DBpediaDataDirCfg.get_instance()
    outdir = cfg.classes

    if not does_result_dir_exist(outdir):
        (
            ontology_dump()
            .get_rdd()
            .filter(is_class)
            .map(to_class)
            .map(lambda c: orjson.dumps(c.to_dict()))
            .saveAsTextFile(
                str(outdir),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset


def is_class(resource: Resource) -> bool:
    return resource.props[RDF.type] == OWL.Class


def to_class(resource: Resource) -> WDClass:
    return WDClass(id=resource.id, label=MultiLingualString())
