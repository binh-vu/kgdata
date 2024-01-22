import re
from functools import lru_cache

from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.ontology_dump import aggregated_triples
from kgdata.misc.ntriples_parser import ignore_comment, ntriple_loads
from kgdata.misc.resource import RDFResource
from kgdata.spark import ExtendedRDD
from kgdata.splitter import split_a_file


@lru_cache()
def mapping_extractor_dump(lang: str = "en") -> Dataset[RDFResource]:
    """
    Splitting dump from DBpedia's MappingExtractor.
    More information: https://databus.dbpedia.org/dbpedia/mappings.

    Returns:
        Dataset[dict]
    """
    cfg = DBpediaDirCfg.get_instance()
    dump_date = cfg.get_dump_date()
    ds = Dataset(
        file_pattern=str(cfg.mapping_extractor_dump / f"final-{lang}" / "*.gz"),
        deserialize=RDFResource.deser,
        name=f"mapping-extractor-dump/{dump_date}-{lang}",
        dependencies=[],
    )
    if not ds.has_complete_data():
        split_dump_dir = cfg.mapping_extractor_dump / f"raw-{lang}"
        for file in cfg.get_mapping_extractor_dump_files(lang):
            split_a_file(
                infile=file,
                outfile=split_dump_dir
                / re.sub(r"[^a-zA-Z]", "-", file.name.split(".", 1)[0])
                / "part.ttl.gz",
                n_writers=8,
                override=False,
            )

        (
            ExtendedRDD.textFile(split_dump_dir / "*/*.gz")
            .filter(ignore_comment)
            .map(ntriple_loads)
            .groupBy(lambda x: x[0])
            .map(aggregated_triples)
            .map(RDFResource.ser)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    return ds
