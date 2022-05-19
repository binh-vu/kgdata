import bz2
import os
import re
import shutil
import warnings
from operator import attrgetter
from pathlib import Path
from typing import List

import numpy as np
from bs4 import Tag, NavigableString
from tqdm.auto import tqdm

from kgdata.config import DBPEDIA_DIR
from kgdata.dbpedia.dbpediamodels import *
from kgdata.dbpedia.dbpediamodels import (
    Table,
)
from kgdata.dbpedia.instances_extraction import (
    merge_triples,
    rdfterm_to_json,
    merged_instances_fixed_wiki_id_en,
)
from kgdata.dbpedia.table_tests import is_relational_table
from kgdata.misc.ntriples_parser import ntriple_loads
from kgdata.spark import (
    get_spark_context,
    ensure_unique_records,
    left_outer_join,
    does_result_dir_exist,
)
from kgdata.wikidata.deprecated.rdd_datasets import wikidata_wikipedia_links
from kgdata.wikipedia.prelude import get_title_from_url, title2groups
from sm.misc.deser import get_open_fn

"""This module provides functions to extract tables from DBPedia raw table dumps.

To extract data until step 2, refer to the bash scripts in scripts/dbpedia/tables.sh
"""


class CounterFlag:
    def __init__(self, name):
        self.name = name
        self.x = 0
        self.y = 0

    def count(self, flag):
        if flag:
            self.x += 1
        self.y += 1

    def __repr__(self):
        return str(self)

    def __str__(self):
        percentage = self.x / self.y if self.y > 0 else 0
        percentage *= 100
        percentage = round(percentage, 2)
        return f"{self.name}: {percentage}% ({self.x}/{self.y})"


class RawHTMLTable:
    def __init__(self, id: str, classes: Set[str], html: str, pageURI: str):
        # spark cannot pickle dataclass because of mappingproxy objects
        self.id = id
        self.classes = classes
        self.html = html
        self.pageURI = pageURI

    @staticmethod
    def deserialize(text: str):
        o = orjson.loads(text)
        o["classes"] = set(o.pop("classes"))
        return RawHTMLTable(**o)

    def serialize(self):
        return orjson.dumps(
            {
                "id": self.id,
                "classes": list(self.classes),
                "html": self.html,
                "pageURI": self.pageURI,
            }
        )


class InvalidCellSpanException(Exception):
    """Indicating that the html colspan or rowspan is wrong"""

    pass


class HTMLTableExtractor:
    NUM_REGEX = re.compile("(\d+)")

    """Extract tables from an html table, which may contains multiple nested tables. I assume that the outer tables are used for
    formatting and the inner tables are the interested ones. So this extractor will discard the outer tables and only keep
    tables at the bottom level.

    The main function of this extractor are extract
    """

    def extract(self, tbl: RawHTMLTable) -> List[Table]:
        """Extract just one html table, which may contains multiple nested tables. I assume that the outer tables are used for
        formatting and the inner tables are the interested ones. So this function will discard the outer tables and only keep
        tables at the bottom level.

        This function right now ignore tables with invalid rowspan or cellspan

        Parameters
        ----------
        tbl : RawHTMLTable
            raw table that we are going to extract

        Returns
        -------
        List[Table]
            list of tables extracted from the input table
        """
        el = BeautifulSoup(tbl.html, "html5lib")
        children = el.find("body").contents
        assert (
            len(children) == 1
        ), "only one table in the body tag of the html document (not table body)"
        results = []
        self._extract_table(tbl, children[0], results)
        return results

    def _extract_table(self, tbl: RawHTMLTable, el: Tag, results: List[Table]):
        """Extract tables from the table tag.

        Parameters
        ----------
        el : Tag
            html table tag
        results : List[Table]
            list of results
        """
        assert el.name == "table"
        caption = None
        rows = []
        contain_nested_table = any(
            c.find("table") is not None is not None for c in el.contents
        )

        for c in el.contents:
            if isinstance(c, NavigableString):
                continue
            if c.name == "caption":
                caption = c.get_text().strip()
                continue
            if c.name == "style":
                continue
            assert (
                c.name == "thead" or c.name == "tbody"
            ), f"not implemented {c.name} tag"
            for row_el in c.contents:
                if isinstance(row_el, NavigableString):
                    continue
                if row_el.name == "style":
                    continue

                assert row_el.name == "tr", f"Invalid tag: {row_el.name}"
                cells = []
                for cell_el in row_el.contents:
                    if isinstance(cell_el, NavigableString):
                        continue
                    if cell_el.name == "style":
                        continue

                    assert (
                        cell_el.name == "th" or cell_el.name == "td"
                    ), f"Invalid tag: {row_el.name}"

                    if contain_nested_table:
                        nested_tables = []
                        self._extract_tbl_tags(cell_el, nested_tables)
                        for tag in nested_tables:
                            self._extract_table(tbl, tag, results)
                    else:
                        try:
                            cell = self._extract_cell(tbl, cell_el)
                            cells.append(cell)
                        except InvalidCellSpanException:
                            # not extracting this table, this table is not table at the intermediate level
                            return

                if not contain_nested_table:
                    rows.append(Row(cells=cells))

        if not contain_nested_table:
            results.append(
                Table(
                    id=tbl.id,
                    pageURI=tbl.pageURI,
                    rows=rows,
                    caption=caption,
                    attrs=el.attrs,
                    classes=tbl.classes,
                )
            )

    def _extract_tbl_tags(self, el: Tag, tags: List[Tag]):
        """Recursive find the first table node

        Parameters
        ----------
        el : Tag
            root node that we start finding
        tags : List[Tag]
            list of results
        """
        for c in el.children:
            if c.name == "table":
                tags.append(c)
            elif isinstance(c, NavigableString):
                continue
            elif len(c.contents) > 0:
                self._extract_tbl_tags(c, tags)

    def _extract_cell(self, tbl: RawHTMLTable, el: Tag) -> Cell:
        """Extract cell from td/th tag. This function does not expect a nested table in the cell

        Parameters
        ----------
        tbl : RawHTMLTable
            need the table to get the page URI in case we have cell reference
        el : Tag
            cell tag (th or td)
        """
        assert el.name == "th" or el.name == "td"

        # extract other attributes
        is_header = el.name == "th"
        attrs = el.attrs

        colspan = attrs.get("colspan", "1").strip()
        rowspan = attrs.get("rowspan", "1").strip()

        if colspan == "":
            colspan = 1
        else:
            m = self.NUM_REGEX.search(colspan)
            if m is None:
                raise InvalidCellSpanException()
            colspan = int(m.group(0))
        if rowspan == "":
            rowspan = 1
        else:
            m = self.NUM_REGEX.search(rowspan)
            if m is None:
                # this is not correct, but
                raise InvalidCellSpanException()
            rowspan = int(m.group(0))

        # extract value
        result = {"text": "", "links": []}
        self._extract_cell_recur(tbl, el, result)
        value, links = result["text"], result["links"]
        links.sort(key=lambda x: x.start)

        # re-adjust ending because we have extra spaces
        value = value.rstrip()
        offset = len(result["text"]) - len(value)
        for link in reversed(links):
            if link.start > len(value):
                link.start -= offset
            if link.end > len(value):
                link.end -= offset
            else:
                break

        return Cell(
            value=value,
            html=str(el),
            links=links,
            is_header=is_header,
            attrs=attrs,
            colspan=colspan,
            rowspan=rowspan,
        )

    def _extract_cell_recur(self, tbl: RawHTMLTable, el: Tag, result: dict):
        """Real implementation of the _extract_cell function"""
        for c in el.children:
            if c.name is None:
                result["text"] += c.strip()
                result["text"] += " "
            elif c.name == "br" or c.name == "hr":
                result["text"] += "\n"
            elif c.name == "a":
                is_selflink = False
                if "href" not in c.attrs:
                    if "selflink" in c["class"]:
                        href = tbl.pageURI
                        is_selflink = True
                    else:
                        raise Exception(f"does not have href: {c}")
                else:
                    href = c.attrs["href"]

                start = len(result["text"])
                updated_index = len(result["links"])
                result["links"].append(
                    Link(href=href, start=start, end=None, is_selflink=is_selflink)
                )

                self._extract_cell_recur(tbl, c, result)
                if len(result["text"]) == start:
                    # link without any text
                    end = len(result["text"])
                else:
                    # -1 for the trailing space
                    if result["text"][-1] == " ":
                        end = len(result["text"]) - 1
                    else:
                        # there still be a chance that may be they just have break line..?
                        end = len(result["text"])
                result["links"][updated_index].end = end
            else:
                self._extract_cell_recur(tbl, c, result)


def raw_tables_en(
    indir: str = os.path.join(DBPEDIA_DIR, "tables_en"),
    infile: str = "step_0/raw-tables_lang=en.ttl.bz2",
):
    """Read tables from dbpedia raw table dumps. Beside the original content, we also generate an ID for the table.

    Parameters
    ----------
    indir : str, optional
        input directory, by default "${DATA_DIR}/dbpedia/tables_en"
    infile : str, optional
        input file, by default "step_0/raw-tables_lang=en.ttl.bz2"
    """
    sc = get_spark_context()

    infile = os.path.join(indir, infile)
    temp_file = os.path.join(indir, "step_1_raw_tables", "step_0")
    outfile = os.path.join(indir, "step_1_raw_tables", "step_1")

    p_html = "http://purl.org/dc/elements/1.1/source"
    p_pageURI = "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#referenceContext"
    p_classes = "http://www.w3.org/1999/xhtml/class"
    p_begin_index = (
        "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#beginIndex"
    )
    p_end_index = (
        "http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#endIndex"
    )
    p_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    predicates = {
        "@id",
        p_html,
        p_classes,
        p_pageURI,
        p_begin_index,
        p_end_index,
        p_type,
    }

    def create_raw_html_table(obj):
        for key, value in obj.items():
            assert key in predicates
            assert not isinstance(value, list)

        assert obj[p_pageURI].endswith("&nif=context")
        # because data type is html
        html = obj[p_html]["value"]

        return RawHTMLTable(
            id=obj["@id"],
            classes=obj[p_classes].split(" "),
            html=html,
            pageURI=str(obj[p_pageURI]),
        )

    if not os.path.exists(temp_file):
        sc.textFile(infile, minPartitions=128).map(ntriple_loads).map(
            lambda x: [str(x[0]), str(x[1]), rdfterm_to_json(x[2])]
        ).map(orjson.dumps).saveAsTextFile(
            temp_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    if not os.path.exists(os.path.join(outfile, "_SUCCESS")):
        if os.path.exists(outfile):
            shutil.rmtree(outfile)

        rdd = (
            sc.textFile(temp_file)
            .map(orjson.loads)
            .map(lambda x: (x[0], x))
            .groupByKey()
            .map(lambda x: merge_triples(list(x[1]), False))
            .map(create_raw_html_table)
            .map(lambda x: x.serialize())
            .saveAsTextFile(
                outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
            )
        )

        ensure_unique_records(sc.textFile(outfile).map(orjson.loads), itemgetter("id"))

    rdd = sc.textFile(outfile).map(RawHTMLTable.deserialize)
    return rdd


def tables_en(
    raw_tables_rdd=None,
    outfile: str = os.path.join(DBPEDIA_DIR, "tables_en/step_2_tables"),
):
    """Get clean tables from dbpedia dumps

    Parameters
    ----------
    raw_tables_rdd : RDD
        input dataset
    outfile : str, optional
        output file, by default "${DATA_DIR}/dbpedia/tables_en/step_2_tables"
    """
    sc = get_spark_context()

    step0_file = os.path.join(outfile, "step_0")
    outfile = os.path.join(outfile, "step_1")

    if not does_result_dir_exist(step0_file):
        raw_tbls = raw_tables_rdd or raw_tables_en()

        def p1_convert_raw_tbl(tbl):
            try:
                tbls = HTMLTableExtractor().extract(tbl)
            except:
                print("Error while extracting tbl", tbl.serialize())
                raise

            if len(tbls) > 0:
                id = tbls[0].id
                assert all(tbl.id == id for tbl in tbls)
                for i, tbl in enumerate(tbls):
                    tbl.id += f"&order={i}"
            return tbls

        raw_tbls.flatMap(p1_convert_raw_tbl).map(Table.ser_bytes).saveAsTextFile(
            step0_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

        tbls_rdd = sc.textFile(step0_file).map(Table.deser_str)
        ensure_unique_records(tbls_rdd, attrgetter("id"))

    if not does_result_dir_exist(outfile):
        tbls_rdd = sc.textFile(step0_file).map(Table.deser_str)

        def p2_span_cells(tbl):
            if len(tbl.rows) == 0:
                return None

            try:
                tbl = tbl.span()
            except OverlapSpanException:
                return None
            except InvalidColumnSpanException:
                return None

            return tbl.pad()

        tbls_rdd.map(p2_span_cells).filter(lambda x: x is not None).map(
            Table.ser_bytes
        ).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    rdd = sc.textFile(outfile).map(Table.deser_str)
    return rdd


def relational_tables_en(
    tables_rdd=None,
    outfile: str = os.path.join(DBPEDIA_DIR, "tables_en/step_3_relational_tables"),
):
    """Extract relational tables from the input tables

    Parameters
    ----------
    tables_rdd : RDD
        input dataset
    outfile : str, optional
        output file, by default "${DATA_DIR}/dbpedia/tables_en/step_3_relational_tables"
    """
    sc = get_spark_context()

    if not does_result_dir_exist(outfile):
        tables_rdd = tables_rdd or tables_en()
        tables_rdd.filter(is_relational_table).map(Table.ser_bytes).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    return sc.textFile(outfile).map(Table.deser_str)


def wikipedia_links_in_relational_tables_en(
    tables_rdd=None,
    outfile: str = os.path.join(
        DBPEDIA_DIR, "tables_en/step_4_wiki_links_in_reltables_en"
    ),
):
    """Get list of wikipedia links in the relational tables including the link of the page containing the table.

    This RDD contains links that have been resolved redirection, so we should use it instead of the default

    Parameters
    ----------
    tables_rdd : RDD, optional
        input tables, by default None
    outfile : str, optional
        output file, by default "${DATA_DIR}/dbpedia/tables_en/step_4_wiki_links_in_reltables_en"

    Returns
    -------
    RDD
        {"title": <title>, "id": <id>, "url": <url> }
    """
    sc = get_spark_context()

    if not does_result_dir_exist(outfile):

        def p_0_get_article_urls(tbl):
            """Get links in a table"""
            urls = set()
            for row in tbl.rows:
                for cell in row.cells:
                    for link in cell.links:
                        if link.is_wikipedia_article_link():
                            urls.add(link.url)
            urls.add(tbl.wikipediaURL)
            return urls

        def p_1_add_title(url):
            title = get_title_from_url(url)
            return title, url

        def p_2_resolve_article_id(x):
            title, (url, wiki_group) = x
            return {
                "url": url,
                # final title
                "title": wiki_group["final"][0],
                "id": wiki_group["final"][1],
            }

        tables_rdd = tables_rdd or relational_tables_en()

        # (title, url)
        links_rdd = (
            tables_rdd.flatMap(p_0_get_article_urls).distinct().map(p_1_add_title)
        )
        links_rdd.join(title2groups()).map(p_2_resolve_article_id).map(
            orjson.dumps
        ).saveAsTextFile(
            outfile, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    return sc.textFile(outfile).map(orjson.loads)


def populated_relational_tables_en(
    tables_rdd=None,
    outfile: str = os.path.join(
        DBPEDIA_DIR, "tables_en/step_5_populated_relational_tables"
    ),
):
    sc = get_spark_context()
    tables_rdd = tables_rdd or relational_tables_en()

    step0_file = os.path.join(outfile, "step_0")
    step1_file = os.path.join(outfile, "step_1")

    if not does_result_dir_exist(step0_file):
        links_rdd = wikipedia_links_in_relational_tables_en()

        def get_title2urls(link):
            url_title = get_title_from_url(link["url"])
            if url_title == link["title"]:
                return [(link["title"], link["url"])]
            return [(url_title, link["url"]), (link["title"], link["url"])]

        title2urls = links_rdd.flatMap(get_title2urls).groupByKey()

        def enwiki_title_to_qnodeid(item):
            if "enwiki" not in item["sitelinks"]:
                return None
            return item["sitelinks"]["enwiki"]["title"], item["id"]

        def to_url2qnode_ids(args):
            title, (qnode_id, urls) = args
            if qnode_id is None:
                return []
            return [(url, (title, qnode_id)) for url in urls]

        def select_correct_qnodes(args):
            # we have wikipedia create a redirect between articles
            # however, some qnodes in wikidata is not updated with the redirection
            # because previously, we keep the title of the URL, and the final title (redirections are resolved)
            # we can try to mitigate this effect a bit
            # but this is still not the correct solution
            url, titles_and_qnodes = args
            url_title = get_title_from_url(url)

            # very few cases like that (25 cases in ~4m)
            # assert len(titles_and_qnodes) == len({x[0] for x in titles_and_qnodes})

            for title, qnode in titles_and_qnodes:
                if title == url_title:
                    return url, [(title, qnode)]
            return url, titles_and_qnodes

        # print(wikidata_wikipedia_links() \
        #     .map(enwiki_title_to_qnodeid) \
        #     .filter(lambda x: x is not None) \
        #     .filter(lambda x: x[1] in  {'Q20965800', 'Q6302754'}) \
        #     .collect())

        # print(title2urls \
        #     .map(lambda x: (x[0], list(x[1]))) \
        #     .filter(lambda x: 'http://en.wikipedia.org/wiki/275264_Krisztike' in x[1]).collect())
        # return
        tmp_rdd = (
            wikidata_wikipedia_links()
            .map(enwiki_title_to_qnodeid)
            .filter(lambda x: x is not None)
            .rightOuterJoin(title2urls)
            .flatMap(to_url2qnode_ids)
            .groupByKey()
            .map(lambda x: (x[0], list(set(x[1]))))
            .map(select_correct_qnodes)
        )

        # tmp_rdd = tmp_rdd.cache()
        # print(tmp_rdd.count(), tmp_rdd.filter(lambda x: len(x[1]) > 1).count())
        # print(tmp_rdd.filter(lambda x: len(x[1]) > 1).take(10))
        # return

        tmp_rdd.map(lambda x: (x[0], x[1][0][1])).map(orjson.dumps).saveAsTextFile(
            step0_file,
            compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
        )

    # print(sc.textFile(step0_file).map(orjson.loads) \
    #     .filter(lambda x: x[1] == 'Q3366494') \
    #     .collect())

    # links_rdd = wikipedia_links_in_relational_tables_en()
    # def get_title2url(link):
    #     url_title = get_title_from_url(link['url'])
    #     if url_title == link['title']:
    #         return [(link['title'], link['url'])]
    #     return [(url_title, link['url']), (link['title'], link['url'])]

    # rdd = links_rdd.flatMap(get_title2url).groupByKey()
    # rdd = rdd.cache()
    # print(rdd.count(), rdd.map(itemgetter(0)).distinct().count())

    if not does_result_dir_exist(step1_file):
        # now create populated tables
        url2qnodeid = sc.broadcast(
            sc.textFile(step0_file).map(orjson.loads).collectAsMap()
        )

        def resolve_wikipedia_links(tbl):
            tbl.external_links = {}
            for row in tbl.rows:
                for cell in row.cells:
                    for link in cell.links:
                        url = link.url
                        if url not in tbl.external_links:
                            qnode_id = url2qnodeid.value.get(url, None)
                            tbl.external_links[url] = ExternalLink(
                                qnode_id=qnode_id, dbpedia=None, qnode=None
                            )

            tbl.external_links[tbl.wikipediaURL] = ExternalLink(
                qnode_id=url2qnodeid.value.get(tbl.wikipediaURL, None),
                dbpedia=None,
                qnode=None,
            )
            return tbl

        tables_rdd.map(resolve_wikipedia_links).map(Table.ser_bytes).saveAsTextFile(
            step1_file, compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec"
        )

    rdd = sc.textFile(step1_file).map(Table.deser_str)
    return rdd


def populated_relational_tables_en_inclusive(
    tables_rdd=None,
    outfile: str = os.path.join(
        DBPEDIA_DIR, "tables_en/step_5_populated_relational_tables"
    ),
    full_qnode: bool = False,
    minimal: bool = False,
):
    """Populate links in the tables with information from DBPedia and Wikidata.

    This dataset now keep all links to DBPedia and Wikidata despite that they may not contain useful entities, i.e.,
    entities that have RDF types.

    This function is deprecated because we put the full dbpedia entities and wikidata entities inside, which make
    the table so big, and we cannot put entities in multiple hop. Use the `populated_relational_tables_en` function istead

    Parameters
    ----------
    indir : str, optional
        input directory of wiki tables, by default "${DATA_DIR}/dbpedia/tables_en"
    outfile : str, optional
        output file, by default "step_5_populated_relational_tables"
    full_qnode: bool, optional
        whether to store full information of qnode
    minimal: bool, optional
        whether to compute a minimal version, which just keep the id
    """
    warnings.warn(
        "populated_relational_tables_en_inclusive is deprecated", DeprecationWarning
    )

    sc = get_spark_context()

    tables_rdd = tables_rdd or relational_tables_en()

    step0_file = os.path.join(outfile, "step_0")
    step1_file = os.path.join(outfile, "step_1")

    assert not (
        full_qnode and minimal
    ), "Cannot compute full and minimal version at the same time"
    if full_qnode:
        step1_file += "_full_qnode"
    if minimal:
        step1_file += "_minimal"

    step00_file = os.path.join(step0_file, "step_0")
    step01_file = os.path.join(step0_file, "step_1")
    step02_file = os.path.join(step0_file, "step_2")

    if not does_result_dir_exist(step0_file):
        # getting urls in all tables, and their correspondent wikipedia id, dbpedia instances and wikidata instances
        def p_1_keep_useful_dbpedia_instance(instance):
            return True
            if "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in instance:
                return False
            return (
                instance["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
                != "http://www.w3.org/2002/07/owl#Thing"
            )

        def p_2_keep_relevant_props_in_dbpedia_instance(instance):
            return {
                k: instance[k]
                for k, rk in [
                    ("@id", "@id"),
                    ("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", "@type"),
                    (
                        "http://dbpedia.org/ontology/wikiPageID",
                        "http://dbpedia.org/ontology/wikiPageID",
                    ),
                ]
                if k in instance
            }

        links_rdd = wikipedia_links_in_relational_tables_en()

        # get dbpedia instances with useful props only
        if not does_result_dir_exist(step00_file):
            merged_instances_fixed_wiki_id_en().filter(
                p_1_keep_useful_dbpedia_instance
            ).map(p_2_keep_relevant_props_in_dbpedia_instance).map(
                lambda x: (x["http://dbpedia.org/ontology/wikiPageID"], x)
            ).map(
                orjson.dumps
            ).saveAsTextFile(
                step00_file,
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )

        db_instances_rdd = sc.textFile(step00_file).map(orjson.loads)

        def p_3_process_dbpedia_join(x):
            _, (link, dbinstance) = x
            return {"link": link, "dbpedia": dbinstance}

        if not does_result_dir_exist(step01_file):
            links_rdd.map(lambda x: (x["id"], x)).leftOuterJoin(db_instances_rdd).map(
                p_3_process_dbpedia_join
            ).map(orjson.dumps).saveAsTextFile(
                step01_file,
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )

        links_and_dbpedia_rdd = sc.textFile(step01_file).map(orjson.loads)

        def p_4_keep_useful_qnode(qnode):
            if "enwiki" not in qnode.sitelinks:
                return False
            return True
            # return 'P31' in qnode.props and len(qnode.props['P31']) > 0

        def p_5_process_wikidata_join(x):
            _, (link, qnode) = x
            if qnode is not None:
                link["qnode"] = asdict(qnode)
            else:
                link["qnode"] = None
            return link

        if not does_result_dir_exist(step02_file):
            # get the list of wikidata and useful props only
            from kgdata.wikidata.deprecated.rdd_datasets import wiki_reltables_instances

            qnodes_rdd = (
                wiki_reltables_instances()
                .filter(p_4_keep_useful_qnode)
                .map(lambda x: (x.sitelinks["enwiki"].title, x))
            )

            links_and_dbpedia_rdd.map(lambda x: (x["link"]["title"], x)).leftOuterJoin(
                qnodes_rdd
            ).map(p_5_process_wikidata_join).map(orjson.dumps).saveAsTextFile(
                step02_file,
                compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
            )

        with open(os.path.join(step0_file, "_SUCCESS"), "w") as f:
            f.write("DONE")

    if not does_result_dir_exist(step1_file):
        # now create populated tables
        if os.path.exists(step1_file):
            shutil.rmtree(step1_file)

        def rdd1_fk_fn(tbl):
            """Get links in a table"""
            urls = set()
            for row in tbl.rows:
                for cell in row.cells:
                    for link in cell.links:
                        if link.is_wikipedia_article_link():
                            urls.add(link.url)
            urls.add(tbl.wikipediaURL)
            return list(urls)

        def rdd1_join_fn(tbl, keys_and_records2):
            url2instance = {}
            non_instance = {"dbpedia": None, "qnode": None}
            for key, r2 in keys_and_records2:
                if r2 is not None:
                    r2id, r2 = r2
                    if full_qnode:
                        o = orjson.loads(bz2.decompress(r2))
                    else:
                        o = r2

                    url2instance[key] = {"dbpedia": o["dbpedia"], "qnode": o["qnode"]}

            tbl.external_links = {}
            for row in tbl.rows:
                for cell in row.cells:
                    for link in cell.links:
                        url = link.url
                        if url not in tbl.external_links:
                            tbl.external_links[url] = url2instance.get(
                                url, non_instance
                            )
            tbl.external_links[tbl.wikipediaURL] = url2instance.get(
                tbl.wikipediaURL, non_instance
            )
            return tbl

        rdd1 = tables_rdd
        rdd1_keyfn = attrgetter("id")
        rdd1_serfn = Table.ser_bytes

        def compress_qnode(r):
            r = orjson.loads(r)
            rid = r["link"]["url"]

            if full_qnode:
                r = bz2.compress(orjson.dumps(r))
            elif minimal:
                r = {
                    "dbpedia": {
                        "@id": r["dbpedia"]["@id"],
                        "@type": r["dbpedia"]["@type"],
                    }
                    if r["dbpedia"] is not None
                    else None,
                    "qnode": {
                        "id": r["qnode"]["id"],
                        "label": r["qnode"]["label"],
                        "props": {
                            k: r["qnode"]["props"][k]
                            for k in ["P31"]
                            if k in r["qnode"]["props"]
                        },
                    }
                    if r["qnode"] is not None
                    else None,
                }
            else:
                if r["qnode"] is not None:
                    qnode = {
                        "id": r["qnode"]["id"],
                        "type": r["qnode"]["type"],
                        "datatype": r["qnode"]["datatype"],
                        "label": r["qnode"]["label"],
                        "description": r["qnode"]["description"],
                        "aliases": r["qnode"]["aliases"],
                        "props": {
                            k: r["qnode"]["props"][k]
                            for k in ["P31"]
                            if k in r["qnode"]["props"]
                        },
                        "sitelinks": {},
                    }
                else:
                    qnode = None
                r = {"dbpedia": r["dbpedia"], "qnode": qnode}

            return (rid, r)

        rdd2 = sc.textFile(step02_file).map(compress_qnode)
        rdd2_keyfn = itemgetter(0)

        left_outer_join(
            rdd1,
            rdd2,
            rdd1_keyfn,
            rdd1_fk_fn,
            rdd2_keyfn,
            rdd1_join_fn,
            rdd1_serfn,
            step1_file,
            compression=True,
        )

    rdd = sc.textFile(step1_file).map(Table.deser_str)
    return rdd


def extract_tables_with_links(infile: str, outdir: str, report: bool = False):
    """Filter tables with links

    Parameters
    ----------
    infile : str
        input json file
    outdir : str
        output directory, filename will be the same as the infile
    report : bool, optional
        report the progress, by default False
    """

    def has_links(tbl):
        for row in tbl.rows:
            for cell in row.cells:
                if len(cell.links) > 0:
                    return True
        return False

    def extract_useful_columns(tbl):
        array = [[c for c in row.cells] for row in tbl.rows[1:]]
        header = np.asarray([c.value for c in tbl.rows[0]])

        try:
            nrows, ncols = tbl.get_shape()
        except:
            return []
        nrows -= 1
        if nrows == 0:
            return []

        useful_columns = []
        for j in range(ncols):
            no_links = 0
            full_cover_links = 0
            for i in range(nrows):
                cell = array[i][j]
                if len(cell.links) == 0 or len(cell.links) > 2:
                    continue

                no_links += 1
                # print((cell.links[-1].end - cell.links[0].start) / len(cell.value), cell.links[0].start, cell.links[-1].end, f"`{cell.value}`", len(cell.value))
                nchars = max(len(cell.value), 1)
                link_fraction = cell.links[-1].end - cell.links[0].start
                if link_fraction / nchars >= 0.9 or nchars - link_fraction <= 4:
                    full_cover_links += 1
            #     if (cell.links[-1].end - cell.links[0].start) / max(len(cell.value), 1) >= 0.8:
            #         full_cover_links += 1

            # print(header[j], no_links / array.shape[0], full_cover_links / array.shape[0])

            if no_links / nrows < 0.5:
                continue
            if full_cover_links / no_links < 0.7:
                continue

            useful_columns.append(j)
        return useful_columns

    assert os.path.exists(outdir), f"Output directory {outdir} does not exist"
    outfile = os.path.join(outdir, Path(infile).name)

    ncols = 0
    with get_open_fn(infile)(infile, "rb") as f:
        for line in tqdm(f, desc=f"extracting({Path(infile).name})") if report else f:
            tbl = Table.deser_str(line)
            # tbl.norm_cells()
            if not has_links(tbl):
                continue
            ncols += len(extract_useful_columns(tbl))
    return ncols


if __name__ == "__main__":
    try:
        rdd = raw_tables_en()
        print(">>> raw_tables_en() done")
        rdd = tables_en()
        print(">>> tables_en() done")
        rdd = relational_tables_en()
        print(">>> relational_tables_en() done")
        rdd = populated_relational_tables_en()
        print(">>> populated_relational_tables_en() done")
    except:
        import traceback

        traceback.print_exc()
        # input(">>> [ERROR] Press any key to finish")
        exit(0)

    # tbl_id = 'http://dbpedia.org/resource/11th_Lok_Sabha?dbpv=2020-02&nif=table&ref=3.10_2&order=0'
    tbl_id = "http://dbpedia.org/resource/59th_General_Assembly_of_Nova_Scotia?dbpv=2020-02&nif=table&ref=3_2&order=0"
    tbl = rdd.filter(lambda tbl: tbl.id == tbl_id).collect()[0]
    # tbl = rdd.filter(lambda x: 'http://en.wikipedia.org/wiki/Blowout_preventer' in x.external_links).take(1)[0]
    # print(tbl.id)
    print(tbl.external_links)
    print(
        tbl.external_links["http://en.wikipedia.org/wiki/Liberal_Party_of_Nova_Scotia"]
    )
    # tbl = rdd.take(1)[0]
    # tbl = wiki_links_in_reltables_en().take(20)
    # import IPython
    # IPython.embed()

    # def fix_id(tbl):
    #     assert tbl.id.find("&nif=context?begin_index=") != -1
    #     tbl.id = tbl.id.replace("&nif=context?begin_index=", "&nif=context&begin_index=")
    #     return tbl

    # rdd.map(fix_id).map(RawHTMLTable.serialize).saveAsTextFile("${DATA_DIR}/dbpedia/tables_en/step_2_new", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
    # tables_en().map(fix_id).map(Table.ser_bytes).saveAsTextFile("${DATA_DIR}/dbpedia/tables_en/step_3_new", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

    # def test(tbl):
    #     assert tbl.pageURI.endswith("&nif=context"), tbl.pageURI
    #     return 1
    # print(rdd.map(test).sum())

    # table_wiki_links_en()
    # populated_relational_tables_en()
    # extract_tables("${DATA_DIR}/dbpedia/tables_en/data_50k.00000.gz", "${DATA_DIR}/dbpedia/tables_en", True)
    # from sm_unk import misc as M
    # tbls_iter = M.get_lines_iter("${DATA_DIR}/dbpedia/raw_tables_en/*.gz").map(M.dbpedia.RawHTMLTable.deser_str)
    # for tbl in tqdm(tbls_iter):
    #     HTMLTableExtractor().extract(tbl)

    # input(">>> Press any key to finish")
