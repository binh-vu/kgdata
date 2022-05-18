import csv
import os
from operator import itemgetter
from typing import Dict, Tuple, Union
from kgdata.spark import does_result_dir_exist, get_spark_context, ensure_unique_records
from kgdata.wikidata.config import WDDataDirCfg
from kgdata.wikidata.datasets.entity_ids import is_entity_id
from kgdata.dataset import Dataset
from kgdata.wikidata.datasets.page_dump import page_dump
from pyspark.rdd import RDD
from sm.misc import deserialize_byte_lines
from tqdm import tqdm


def page_ids() -> Dataset[Tuple[str, str]]:
    """Get mapping from Wikidata internal page id and Wikidata entity id (possible old id).
    To use the entity id, we should use it with redirections (`entity_redirections`)

    Pages may contain user pages, etc. So we are only keep pages that have entity ids.

    Returns:
        Dataset[tuple[str, str]]
    """
    cfg = WDDataDirCfg.get_instance()

    if not does_result_dir_exist(cfg.page_ids):
        (
            page_dump()
            .get_rdd()
            .flatMap(parse_sql_values)
            .map(extract_id)
            .filter(lambda x: x is not None)
            .map(lambda x: "\t".join(x))
            .saveAsTextFile(
                str(cfg.page_ids),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    if not (cfg.page_ids / "_METADATA").exists():
        rdd = (
            get_spark_context()
            .textFile(os.path.join(cfg.page_ids, "*.gz"))
            .map(lambda x: x.split("\t"))
        )
        ensure_unique_records(
            rdd,
            itemgetter(0),
        )
        (cfg.page_ids / "_METADATA").write_text(
            f"""
key.unique: true
n_records: {(rdd.count())}
            """.strip()
        )

    return Dataset(
        file_pattern=cfg.page_ids / "*.gz", deserialize=lambda x: tuple(x.split("\t"))
    )


def extract_id(row: list):
    # the dumps contain other pages such as user pages, etc.
    page_id, entity_id = row[0], row[2]
    if not is_entity_id(entity_id):
        return None

    assert page_id.isdigit(), page_id
    return page_id, entity_id


def parse_sql_values(line):
    values = line[line.find("` VALUES ") + 9 :]
    latest_row = []
    reader = csv.reader(
        [values],
        delimiter=",",
        doublequote=False,
        escapechar="\\",
        quotechar="'",
        strict=True,
    )

    output = []
    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == "NULL":
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    output.append(latest_row)
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            output.append(latest_row)
    return output
