import orjson, os

from kgdata.config import WIKIPEDIA_DIR
from kgdata.spark import get_spark_context, ensure_unique_records, itemgetter


"""Containing Spark RDD datasets that is built from extracted files in `wikidump_io` module
"""


def title2groups(infile: str=os.path.join(WIKIPEDIA_DIR, "pages_articles_en/article_groups/*.gz")):
    """Get RDD that maps from article title into a cluster of articles

    Parameters
    ----------
    infile : str, optional
        input file obtained from group_pages function, by default "${DATA_DIR}/wikipedia/pages_articles_en/article_groups/*.gz"

    Returns
    -------
    RDD:
        (title, { "final": (<title>, <wiki_id>), "group": [(<title>, <wiki_id>)] })
    """
    sc = get_spark_context()
    return sc.textFile(infile).map(orjson.loads).flatMap(lambda x: [(title, x) for title, id in x['group']])


def id2groups(infile: str=os.path.join(WIKIPEDIA_DIR, "pages_articles_en/article_groups/*.gz")):
    """Get RDD that maps from article id into a cluster of articles

    Parameters
    ----------
    infile : str, optional
        input file obtained from group_pages function, by default "${DATA_DIR}/wikipedia/pages_articles_en/article_groups/*.gz"

    Returns
    -------
    RDD:
        (id, { "final": (<title>, <wiki_id>), "group": [(<title>, <wiki_id>)] })
    """
    sc = get_spark_context()
    return sc.textFile(infile).map(orjson.loads).flatMap(lambda x: [(id, x) for title, id in x['group']])


if __name__ == "__main__":
    rdd = title2groups()
    # rdd = id2groups()
    ensure_unique_records(rdd, itemgetter(0))