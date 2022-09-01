from kgdata.wikipedia.datasets.html_tables import deser_table, html_tables, ser_table
from kgdata.dataset import Dataset
from kgdata.spark import does_result_dir_exist
from kgdata.wikipedia.config import WPDataDirConfig
from rsoup.rsoup import Table
import sm.misc as M


def relational_tables() -> Dataset[Table]:
    cfg = WPDataDirConfig.get_instance()

    if not does_result_dir_exist(cfg.relational_tables):
        (
            html_tables()
            .get_rdd()
            .filter(is_relational_table)
            .map(ser_table)
            .coalesce(1024, shuffle=True)
            .saveAsTextFile(
                str(cfg.relational_tables),
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )
        )

    return Dataset(
        file_pattern=cfg.relational_tables / "*.gz",
        deserialize=deser_table,
    )


def is_relational_table(tbl: Table) -> bool:
    if len(tbl.rows) == 0:
        return False

    rows = tbl.rows
    n_headers = 0
    for i in range(len(rows) - 1):
        if not all(c.is_header for c in rows[i].cells):
            break
        n_headers += 1

    if n_headers == 0:
        return False

    for i in range(n_headers, len(rows)):
        if not all(not c.is_header for c in rows[i].cells):
            return False

    return True


if __name__ == "__main__":
    WPDataDirConfig.init("/nas/ckgfs/users/binhvu/wikipedia/20220420")
    relational_tables()
