from kgdata.wikipedia.datasets.html_tables import deser_table, html_tables, ser_table
import orjson
from typing import TypedDict, Union
from loguru import logger
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikipedia.datasets.html_articles import html_articles
from kgdata.wikipedia.config import WPDataDirConfig
from kgdata.wikipedia.models.html_article import HTMLArticle
from table_extractor.table_extractor import HTMLTableExtractor
from table_extractor.models.html_table import HTMLTable
import sm.misc as M


def relational_tables() -> Dataset[HTMLTable]:
    cfg = WPDataDirConfig.get_instance()

    if not does_result_dir_exist(cfg.relational_tables):
        (
            html_tables()
            .get_rdd()
            .filter(is_relational_table)
            .map(ser_table)
            .saveAsTextFile(
                str(cfg.relational_tables),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        file_pattern=cfg.relational_tables / "*.gz",
        deserialize=deser_table,
    )


def is_relational_table(tbl: HTMLTable) -> bool:
    if len(tbl.rows) == 0:
        return False

    if not all(c.is_header for c in tbl.rows[0].cells):
        return False

    if not all(not c.is_header for r in tbl.rows[1:] for c in r.cells):
        return False

    return True


if __name__ == "__main__":
    WPDataDirConfig.init("/nas/ckgfs/users/binhvu/wikipedia/20220420")
    relational_tables()
