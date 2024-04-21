from __future__ import annotations

from kgdata.dataset import Dataset
from kgdata.dbpedia.datasets.ontology_dump import aggregated_triples
from kgdata.misc.ntriples_parser import ignore_comment, ntriple_loads
from kgdata.misc.resource import RDFResource
from kgdata.spark import ExtendedRDD
from kgdata.spark.common import does_result_dir_exist
from kgdata.splitter import split_a_file
from kgdata.wikidata.config import WikidataDirCfg


def triple_truthy_dump() -> Dataset[RDFResource]:
    """Splitting nt truthy dumps of Wikidata from https://zenodo.org/records/7829583"""

    cfg = WikidataDirCfg.get_instance()
    dump_date = cfg.get_dump_date()

    split_dump_dir = cfg.triple_truthy_dump / "raw"
    final_dump_dir = cfg.triple_truthy_dump / "final"
    ds = Dataset(
        file_pattern=final_dump_dir / "*.zst",
        deserialize=RDFResource.deser,
        name=f"triple-truthy-dump/{dump_date}",
        dependencies=[],
    )

    if not ds.has_complete_data():
        if not does_result_dir_exist(split_dump_dir):
            raise Exception(
                f"{split_dump_dir} does not exists. "
                "Manually splitting truthy dump is much faster: `lbzip2 -cd ../../000_dumps/wikidata-20220521-truthy.nt.bz2 | split -d -l1000000 --suffix-length 5 --filter 'zstd -q -6 -o $FILE.zst' - part-`"
            )
            split_a_file(
                infile=cfg.get_triple_truthy_dump_file(),
                outfile=split_dump_dir / "part-*.zst",
                n_writers=16,
                override=False,
                compression_level=9,
            )

        (
            ExtendedRDD.textFile(split_dump_dir / "*.zst")
            .filter(ignore_comment)
            .map(ntriple_loads)
            .groupBy(lambda x: x[0])
            .map(aggregated_triples)
            .map(RDFResource.ser)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )
    return ds
