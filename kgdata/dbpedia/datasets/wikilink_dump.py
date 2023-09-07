
from kgdata.dataset import Dataset
from kgdata.dbpedia.config import DBpediaDirCfg
from kgdata.splitter import split_a_file


def wikilink_dump(lang: str = "en") -> Dataset[str]:
    """
    Split the DBpedia wikilink dump into smaller files.

    Returns:
        Dataset[dict]
    """
    cfg = DBpediaDirCfg.get_instance()

    split_a_file(
        infile=cfg.get_wikilink_dump_file(lang),
        outfile=cfg.wikilink_dump / lang / "part.ndjson.gz",
        n_writers=8,
        override=False,
    )

    return Dataset.string(file_pattern=str(cfg.wikilink_dump / lang / "*.gz"))
