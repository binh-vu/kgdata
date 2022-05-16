import functools
import os
from pathlib import Path
from typing import Union

from kgdata.config import WIKIDATA_DIR
from kgdata.spark import does_result_dir_exist, get_spark_context
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entity_dump import entity_dumps
from kgdata.wikidata.models.wdentity import WDEntity
import orjson


def entities(
    lang: str = "en",
    enforce_consistency: bool = False,
):
    """Normalize Wikidata entity from Wikidata entity json dumps.

    This data does not verify if references within entities are valid. For example,
    an entity may have a property value that links to another entity that does not exist.

    Returns:
        RDD[WDEntity]
    """
    cfg = WDDataDirCfg.get_instance()

    outdir = os.path.join(
        str(cfg.entities) + f"_{lang}",
        "verified" if enforce_consistency else "unverified",
    )

    if not does_result_dir_exist(outdir):
        (
            entity_dumps()
            .map(functools.partial(WDEntity.from_wikidump, lang=lang))
            .map(WDEntity.to_dict)
            .map(orjson.dumps)
            .saveAsTextFile(
                outdir,
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return (
        get_spark_context()
        .textFile(os.path.join(outdir, "*.gz"))
        .map(orjson.loads)
        .map(WDEntity.from_dict)
    )


if __name__ == "__main__":
    WDDataDirCfg.init("/data/binhvu/sm-dev/data/wikidata/20211213")
    print("Total:", entities().count())
