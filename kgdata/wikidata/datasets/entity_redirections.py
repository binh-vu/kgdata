from operator import itemgetter

from kgdata.config import WIKIDATA_DIR
from kgdata.spark import (
    does_result_dir_exist,
    get_spark_context,
    head,
    saveAsSingleTextFile,
)
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entity_dump import entity_dump
from kgdata.wikidata.datasets.entity_ids import is_entity_id
from kgdata.wikidata.datasets.entity_redirection_dump import entity_redirection_dump
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.datasets.page_ids import page_ids, parse_sql_values
import orjson
from sm.misc import identity_func


def entity_redirections():
    """Wikidata entity redirections"""
    cfg = WDDataDirCfg.get_instance()
    sc = get_spark_context()

    if not does_result_dir_exist(cfg.entity_redirections / "raw"):
        # mapping from page id to list previous entity ids
        (
            entity_redirection_dump()
            .flatMap(parse_sql_values)
            .map(extract_id)
            .filter(lambda x: x is not None)
            .groupByKey()
            .map(lambda x: (x[0], list(x[1])))
            .map(orjson.dumps)
            .saveAsTextFile(
                str(cfg.entity_redirections / "raw"),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not does_result_dir_exist(cfg.entity_redirections / "redirections"):
        page2ent = {k: v for k, v in page_ids().collect()}
        redirect_page2ents = {
            k: v
            for k, v in (
                sc.textFile(str(cfg.entity_redirections / "redirections/*.gz"))
                .map(orjson.loads)
                .collect()
            )
        }

        import IPython

        IPython.embed()


def extract_id(row: list):
    # the dumps contain other pages such as user pages, etc.
    page_id, entity_id = row[0], row[2]
    if is_entity_id(entity_id):
        return page_id, entity_id

    return None


def postprocess_join(record):
    k, (v, w) = record
    try:
        assert isinstance(v, list), v
        assert w is not None, (k, v, w)
    except:
        print(record)
        raise
    return record
