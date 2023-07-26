import orjson
from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.dbpedia.datasets.ontology_dump import aggregated_triples
from kgdata.misc.ntriples_parser import Triple, ignore_comment, ntriple_loads
from kgdata.misc.resource import RDFResource
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.splitter import split_a_file


def generic_extractor_dump(lang: str = "en") -> Dataset[RDFResource]:
    """
    Splitting dump from DBpedia's GenericExtractor.
    More information: https://databus.dbpedia.org/dbpedia/generic.

    Returns:
        Dataset[dict]
    """
    cfg = DBpediaDirCfg.get_instance()

    if not does_result_dir_exist(cfg.generic_extractor_dump / lang / "final"):
        split_dump_dir = cfg.generic_extractor_dump / lang / "raw"
        for file in cfg.get_generic_extractor_dump_files(lang):
            split_a_file(
                infile=file,
                outfile=split_dump_dir / f"{file.stem}.part.gz",
                n_writers=8,
                override=False,
            )

        (
            get_spark_context()
            .textFile(str(split_dump_dir / "*.gz"))
            .filter(ignore_comment)
            .map(ntriple_loads)
            .groupBy(lambda x: x[0])
            .map(aggregated_triples)
            .map(RDFResource.ser)
            .saveAsTextFile(
                str(cfg.generic_extractor_dump / lang / "final"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset.string(
        file_pattern=str(cfg.generic_extractor_dump / lang / "final" / "*.gz")
    ).map(RDFResource.deser)
