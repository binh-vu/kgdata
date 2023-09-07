
from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.splitter import split_a_file


def page_id_dump(lang: str = "en") -> Dataset[str]:
    """
    Split the DBpedia page id dump into smaller files.

    Returns:
        Dataset[dict]
    """
    cfg = DBpediaDirCfg.get_instance()
    dump_date = cfg.get_dump_date()
    ds = Dataset.string(
        file_pattern=str(cfg.page_id_dump / lang / "*.gz"),
        name=f"page-id-dump/{dump_date}-{lang}",
        dependencies=[],
    )

    if not ds.has_complete_data():
        split_a_file(
            infile=cfg.get_page_id_dump_file(lang),
            outfile=cfg.page_id_dump / lang / "part.ndjson.gz",
            n_writers=8,
            override=False,
        )
        ds.sign(ds.get_name(), ds.get_dependencies())

    return ds
