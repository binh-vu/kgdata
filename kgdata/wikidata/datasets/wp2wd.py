from typing import Tuple

import orjson
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entities import entities
from kgdata.wikidata.models.wdentity import WDEntity


def wp2wd(lang="en") -> Dataset[Tuple[str, str]]:
    """Get alignments between wiki article titles and wikidata qnode."""
    cfg = WikidataDirCfg.get_instance()
    site = lang + "wiki"

    if not does_result_dir_exist(cfg.wp2wd / lang):
        (
            entities(lang)
            .get_rdd()
            .map(lambda x: extract_link(x, site))
            .filter(lambda x: x is not None)
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.wp2wd / lang),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(cfg.wp2wd / lang / "*.gz", deserialize=lambda x: orjson.loads(x))


def extract_link(ent: WDEntity, site: str):
    if site not in ent.sitelinks:
        return None
    title = ent.sitelinks[site].title
    assert title is not None and isinstance(title, str) and len(title) > 0
    return title, ent.id
