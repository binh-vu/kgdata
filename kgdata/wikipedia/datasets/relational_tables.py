from rsoup.core import Table

from kgdata.dataset import Dataset
from kgdata.wikipedia.config import WikipediaDirCfg
from kgdata.wikipedia.datasets.html_tables import deser_table, html_tables, ser_table


def relational_tables() -> Dataset[Table]:
    cfg = WikipediaDirCfg.get_instance()
    ds = Dataset(
        file_pattern=cfg.relational_tables / "*.gz",
        deserialize=deser_table,
        name="relational-tables",
        dependencies=[html_tables()],
    )

    if not ds.has_complete_data():
        (
            html_tables()
            .get_extended_rdd()
            .filter(is_relational_table)
            .map(ser_table)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )

    return ds


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
    WikipediaDirCfg.init("/nas/ckgfs/users/binhvu/wikipedia/20220420")
    relational_tables()
