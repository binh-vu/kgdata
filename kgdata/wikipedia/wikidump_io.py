import glob
import orjson
import ujson

from kgdata.wikipedia.wikimodels import *
from sm.misc.deser import get_open_fn

"""Module containing code for reading and extracting wikipedia dump from DBPedia at 2016 so it is easier for us to work with
"""

logger = logging.getLogger("wikipedia")


def iter_page_articles(compressed_file: str):
    """Iterate through each page and yield the content.

    Parameters
    ----------
    compressed_file : str
        path to the `page_articles_en.xml.bz2`. Must contains only one latest revision per page. The compression format is going to be determined from the extension (.gz or .bz2)
    """
    ET.register_namespace("", "http://www.mediawiki.org/xml/export-0.10/")

    with get_open_fn(compressed_file)(compressed_file, "r") as f:
        tree = iter(
            ET.iterparse(
                f,
                events=(
                    "start",
                    "end",
                ),
            )
        )
        event, root = next(tree)

        for event, elem in tree:
            if (
                event != "end"
                or elem.tag != "{http://www.mediawiki.org/xml/export-0.10/}page"
            ):
                continue

            id = elem.find("{http://www.mediawiki.org/xml/export-0.10/}id").text
            ns = elem.find("{http://www.mediawiki.org/xml/export-0.10/}ns").text
            title = elem.find("{http://www.mediawiki.org/xml/export-0.10/}title").text
            redirect_title = elem.find(
                "{http://www.mediawiki.org/xml/export-0.10/}redirect"
            )
            if redirect_title is not None:
                redirect_title = redirect_title.get("title")
            model = elem.find(
                "{http://www.mediawiki.org/xml/export-0.10/}revision/{http://www.mediawiki.org/xml/export-0.10/}model"
            ).text
            format = elem.find(
                "{http://www.mediawiki.org/xml/export-0.10/}revision/{http://www.mediawiki.org/xml/export-0.10/}format"
            ).text
            text = (
                elem.find(
                    "{http://www.mediawiki.org/xml/export-0.10/}revision/{http://www.mediawiki.org/xml/export-0.10/}text"
                ).text
                or ""
            )

            yield WikiPageArticle(id, ns, title, redirect_title, model, format, text)

            # Article to avoid using too much memory: https://www.ibm.com/developerworks/xml/library/x-hiperfparse/
            # elem.clear()
            # leverage the fact that pages_articles if mediawiki > page
            root.remove(elem)

        del tree


def split_page_articles(bz2_infile: str, outdir: str, pages_per_file: int = 50000):
    """Split the big `page_articles_en.xml.bz2` to multiple files (compressed in gzip) for faster processing

    Parameters
    ----------
    bz2_infile : str
        path to the `page_articles_en.xml.bz2`
    outdir : str
        path to the out files. the splitted files will automatically be `data_<pages_per_files>.<chunk_id>` before the `gz` extension
    pages_per_file : int, optional
        numbers of pages per file, by default 50000
    """
    # get human readable size
    ppf = pages_per_file
    units = ["", "k", "m", "b"]
    i = 0
    while ppf >= 1000:
        ppf = ppf / 1000
        i += 1

    assert ppf == int(ppf), "Please choose a better number"
    assert Path(outdir).parent.exists(), "Invalid output directory"

    ppf = f"{int(ppf)}{units[i]}"
    filename_format = os.path.join(outdir, f"data_{ppf}.%05d.gz")

    # this seems to be the only way to specify default namespace in python 3.7
    ET.register_namespace("", "http://www.mediawiki.org/xml/export-0.10/")

    with tqdm(desc="split_page_articles") as pbar:
        with get_open_fn(bz2_infile)(bz2_infile, "r") as f:
            batch = []
            file_counter = 0

            tree = iter(
                ET.iterparse(
                    f,
                    events=(
                        "start",
                        "end",
                    ),
                )
            )
            event, root = next(tree)

            for event, elem in tree:
                if (
                    event != "end"
                    or elem.tag != "{http://www.mediawiki.org/xml/export-0.10/}page"
                ):
                    continue

                batch.append(elem)
                pbar.update(1)

                # avoid using too much memory, remove the elem. leverage the fact that pages_articles if mediawiki > page
                root.remove(elem)

                if len(batch) > pages_per_file:
                    subtree = ET.Element("mediawiki")
                    for el in batch:
                        subtree.append(el)
                    outfile = filename_format % file_counter
                    with gzip.open(outfile, "wb") as f:
                        ET.ElementTree(subtree).write(f, encoding="utf-8")
                    batch = []
                    del subtree
                    file_counter += 1

            if len(batch) > 0:
                subtree = ET.Element("mediawiki")
                for el in batch:
                    subtree.append(el)
                outfile = filename_format % file_counter
                with gzip.open(outfile, "wb") as f:
                    ET.ElementTree(subtree).write(f, encoding="utf-8")
                file_counter += 1
                batch = []
                del subtree


def extract_raw_tables(
    infile: str, outfile: str, max_pages: Optional[int] = None, report: bool = False
):
    """Extract tables from infile, the infile has to be compatible with the one using in the `iter_page_articles` function.

    Parameters
    ----------
    infile : str
        path to the `page_articles_en.xml`. Must contains only one latest revision per page. The compression format is going to be determined from the extension (.gz or .bz2)
    outfile : str
        path to the output file, compressed using gzip and in JSON format
    max_pages : Optional[int], optional
        [description], by default None
    """
    if max_pages is None:
        max_pages = float("inf")
    if report:
        pbar = tqdm(desc=f"Extract tables in file {Path(infile).stem}")

    with gzip.open(outfile, "wb") as f:
        counter = 0
        for page in iter_page_articles(infile):
            tables = _extract_raw_tables(page)
            if len(tables) == 0:
                continue

            record = WikiPageExtractedTables(
                page.id, page.ns, page.title, page.text, tables
            )
            f.write(ujson.dumps(asdict(record)).encode())
            f.write(b"\n")
            counter += 1

            if report:
                pbar.update(1)

            if counter >= max_pages:
                break

    if report:
        pbar.close()


def _extract_raw_tables(
    page: WikiPageArticle, silent: bool = True, log_error: bool = False
):
    """Extract tables from an article. This function does not return nested tables as it is included in the outer table

    Parameters
    ----------
    page : WikiPageArticle
        markup content of a wikipedia article
    silent: bool
        whether throwing error when encounter inconsistent open/close tag
    log_error: bool
        whether we will log the error

    Returns
    -------
    List[str]
        list of the raw tables in wikipedia
    """
    otag, ctag = "{|", "|}"
    tables = []
    tag_indices = []
    text = page.text

    for m in re.finditer("{\|", text):
        tag_indices.append((m.start(0), otag))

    # no specified in any wikipedi
    for m in re.finditer("\|}(?!})", text):
        tag_indices.append((m.start(0), ctag))

    tag_indices.sort(key=itemgetter(0))
    if len(tag_indices) == 0:
        return tables

    # in case there is an error in the markup
    i = 0
    while i < len(tag_indices) and tag_indices[i][1] == ctag:
        i += 1

    if i >= len(tag_indices):
        if log_error:
            logger.debug(
                "tag-close inconsistent: url=%s\ntags=%s\n-------content=%s\n-------",
                page.url,
                tag_indices,
                text,
            )
        if not silent:
            raise Exception("Inconsistent between open tag and close tag")

        return tables

    stack = [tag_indices[i][0]]
    for idx, tag in islice(tag_indices, i + 1, None):
        if tag == otag:
            stack.append(idx)
        else:
            # close tag
            if len(stack) == 0:
                # error in the markup, encounter it on: https://en.wikipedia.org/wiki/Template:Manx_Electric_Railway?curid=14227720
                if log_error:
                    logger.debug(
                        "tag-close inconsistent: url=%s\ntags=%s\n-------content=%s\n-------",
                        page.url,
                        tag_indices,
                        text,
                    )
                if not silent:
                    raise Exception("Inconsistent between open tag and close tag")
            elif len(stack) == 1:
                # yield the most outer table
                tables.append(text[stack.pop() : idx + 2])
            else:
                stack.pop()

    return tables


def iter_raw_tables(infile: str):
    """Iterate through each page that contains extracted raw tables. Normally, we use this function after run `extract_raw_tables`

    Parameters
    ----------
    infile : str
        input file
    """
    with get_open_fn(infile)(infile, "rb") as f:
        for line in f:
            yield WikiPageExtractedTables(**ujson.loads(line))


def extract_page_identifications(infile: str, outfile: str):
    with get_open_fn(outfile)(outfile, "wb") as f:
        for page in iter_page_articles(infile):
            f.write(
                orjson.dumps(
                    {
                        "id": int(page.id),
                        "ns": page.ns,
                        "redirect_title": page.redirect_title,
                        "title": page.title,
                    }
                )
            )
            f.write(b"\n")


def group_pages(infile: str, outdir: str):
    """Group wikipedia pages/articles that are belong to the same entity

    Parameters
    ----------
    infile : str
        patterns to find extracted pages
    outdir : str
        output directory
    """
    infiles = sorted(glob.glob(infile))
    wiki_links = []
    for infile in tqdm(infiles, desc="read file"):
        with get_open_fn(infile)(infile, "rb") as f:
            for line in f:
                r = orjson.loads(line)
                wiki_links.append((r["id"], r["title"], r["redirect_title"]))

    # verify if we have the case of one source node is link to two target nodes, then we build dict that manually curate those nodes
    tmp = {}
    manually_curated_source2target = {}
    title2id = {}
    for source_id, source, target in tqdm(wiki_links):
        if source not in tmp:
            tmp[source] = target
            title2id[source] = source_id
        else:
            assert source not in manually_curated_source2target
            if target is None:
                manually_curated_source2target[source] = tmp[source]
                # don't have to update the id since this we discard this article
            else:
                manually_curated_source2target[source] = target
                title2id[source] = source_id

            print("`%s` | `%s` | `%s`" % (source, target, tmp[source]))

    # build reverse map
    reverse_map = {}
    leaves = set()
    for source_id, source, target in tqdm(wiki_links, desc="build reverse map"):
        if source in manually_curated_source2target:
            continue

        if target is None:
            assert source not in leaves
            leaves.add(source)
            continue

        if target not in reverse_map:
            reverse_map[target] = [source]
        else:
            reverse_map[target].append(source)

    for source, target in manually_curated_source2target.items():
        if target is None:
            leaves.add(source)
            continue
        if target not in reverse_map:
            reverse_map[target] = [source]
        else:
            reverse_map[target].append(source)

    # now travel upward to group
    visited = set()

    def trace_upward(reverse_map, group, ptr):
        assert ptr not in visited
        visited.add(ptr)

        for parent in reverse_map.get(ptr, []):
            group.append((parent, title2id[parent]))
            trace_upward(reverse_map, group, parent)

    groups = []
    for leaf in tqdm(leaves, desc="grouping"):
        if leaf not in reverse_map:
            groups.append(
                {"final": (leaf, title2id[leaf]), "group": [(leaf, title2id[leaf])]}
            )
        else:
            group = [(leaf, title2id[leaf])]
            trace_upward(reverse_map, group, leaf)
            groups.append({"group": group, "final": (leaf, title2id[leaf])})

    # write result
    batch_size = 50000
    count = 0
    for i in tqdm(range(0, len(groups), batch_size), desc="writing result"):
        with gzip.open(os.path.join(outdir, "data.%05d.gz" % count), "wb") as f:
            for g in groups[i : i + batch_size]:
                f.write(orjson.dumps(g))
                f.write(b"\n")
            count += 1


if __name__ == "__main__":
    count = 0
    # for page in tqdm(iter_page_articles("/workspace/sm-dev/data/wikipedia/pages_articles_en/step_1/data_50k.00000.gz")):
    #     count += 1
    # extract_raw_tables("./data/wikitables/pages_articles_en/data_50k.00100.gz", "./data/wikitables/pages_articles_en_tables/data_50k.00100.gz", report=True)
    # extract_redirect_links(
    #     "/home/rook/workspace/sm-dev/data/wikipedia/pages_articles_en/step_1/data_50k.00000.gz"
    # )
    group_pages(
        "/workspace/sm-dev/data/wikipedia/pages_articles_en/identifications/*.gz",
        "/workspace/sm-dev/data/wikipedia/pages_articles_en/article_groups",
    )
