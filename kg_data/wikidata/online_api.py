import ujson, time, rdflib, os, logging
from tqdm.auto import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON
from typing import *
from datetime import datetime
from pathlib import Path
from ruamel.yaml import YAML

"""
Functions for querying Wikidata through their SPARQLEndpoint. 

Based on the scripts wikidata_download and wikidata_read in `sm-unknown/scripts`
"""

logger = logging.getLogger("kg_data.wikidata")

# the endpoint that we are going to query
# SPARQL_ENDPOINT = os.environ.get('WIKIDATA_SPARQL_ENDPOINT', "https://query.wikidata.org/sparql")
SPARQL_ENDPOINT = "https://dsbox02.isi.edu:8888/bigdata/namespace/wdq/sparql"
logger.info(f"Going to use {SPARQL_ENDPOINT} to query wikidata")
# a predicate telling that we've got all direct predicates of an entity at a specific time
ACQUIRED_FULL_ONEHOP = "https://isi.edu/ontology/sm/acquired_full_onehop"


with open(str(Path(os.path.abspath(__file__)).parent / "wikidata_prefixes.yml"), "r") as f:
    WIKIDATA_PREFIXES = dict(YAML().load(f))
    WIKIDATA_INVERT_PREFIXES = {v: k for k, v in WIKIDATA_PREFIXES.items()}
assert len(WIKIDATA_PREFIXES) == len(WIKIDATA_INVERT_PREFIXES)


def get_abs_url(url: str) -> str:
    global WIKIDATA_PREFIXES
    if url.startswith("http://") or url.startswith("https://"):
        return url
    prefix, url = url.split(":", 1)
    return WIKIDATA_PREFIXES[prefix] + url


def get_rel_url(url: str) -> str:
    global WIKIDATA_INVERT_PREFIXES
    if url.startswith("http://") or url.startswith("https://"):
        x, y = url.rsplit("/", 1)
        return f"{WIKIDATA_INVERT_PREFIXES[x + '/']}:{y}"
    return url


def query_wikidata(query: str, return_format: Optional[str]="json", endpoint=SPARQL_ENDPOINT):
    global WIKIDATA_PREFIXES

    query = "\n".join([f"PREFIX {ns}: <{url}>" for ns, url in WIKIDATA_PREFIXES.items()]) + "\n" + query
    start = time.time()
    sparql = SPARQLWrapper(endpoint)
    sparql.setMethod("POST")
    sparql.setQuery(query)
    if return_format is not None:
        sparql.setReturnFormat(return_format)

    result = sparql.query().convert()
    print(f">>> Finish the query in {time.time() - start:.5f}")
    return result


def download_entity_urls(conditions: List[Tuple[str, str]], outfile: str, limit: Optional[int]=None, offset: int=0):
    """
    Download entity's URLs

    @param conditions: list of <p>, <o> condition
    @param outfile: the file that we are going to append triples to
    @param limit
    @param offset
    """
    global SPARQL_ENDPOINT, WIKIDATA_PREFIXES
    conditions = [
        f"<{get_abs_url(p)}> <{get_abs_url(o)}>"
        for p, o in conditions
    ]
    query = f"""
        select distinct ?s
        where {{ ?s {"; ".join(conditions)}. }}
        {f"LIMIT {limit}" if limit is not None else ""}
        OFFSET {offset}
    """

    start = time.time()
    # setup and send query
    sparql = SPARQLWrapper(SPARQL_ENDPOINT)
    sparql.setQuery(query)
    sparql.setReturnFormat("json")
    result = sparql.query().convert()
    print(f">>> Finish the query in {time.time() - start:.5f}")

    triples = []   # triples that is going to write to outfile
    ent_uris = []  # list of entity URLs

    # parse result
    for subj in (o['s'] for o in result['results']['bindings']):
        assert subj['type'] == 'uri'
        ent_uris.append(subj['value'])

        for po in conditions:
            triples.append(f"<{subj['value']}> {po} .\n")

    # write triples to file
    with open(outfile, "a") as f:
        for triple in triples:
            f.write(triple)
    return ent_uris


def download_entity_properties(ent_urls: List[str], outfile: str, batch_size: int=5):
    """
    Query entity's properties

    @param outfile: the file that we are going to append triples to
    """
    global SPARQL_ENDPOINT, WIKIDATA_PREFIXES, ACQUIRED_FULL_ONEHOP
    batch_ent_urls = [
        [f"<{get_abs_url(x)}>" for x in ent_urls[i:i+batch_size]]
        for i in range(0, len(ent_urls), batch_size)
    ]

    for burls in tqdm(batch_ent_urls):
        curr_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        sparql = SPARQLWrapper(SPARQL_ENDPOINT)
        sparql.setQuery(f"""
            DESCRIBE ?item
            WHERE {{ VALUES ?item {{ {" ".join(burls)} }} }}
        """)
        sparql.setReturnFormat("rdf+xml")
        g = sparql.query().convert()
        triples = g.serialize(format="ntriples").decode()
        with open(outfile, "a") as f:
            f.write(triples)
            for url in burls:
                f.write(f"{url} <{ACQUIRED_FULL_ONEHOP}> \"{curr_time}^^<http://www.w3.org/2001/XMLSchema#dateTime>\" .\n")


def download_predicates(pred_urls: List[str], outfile: str, batch_size: int=5, filter_duplication: bool=True):
    """
    Download wikidata property ONLY.
    """
    # in wikidata, to get info about a predicate, we have to query its entity
    ent_preds = set()
    for pred in pred_urls:
        if pred.startswith("http://www.wikidata.org/prop/P") or pred.startswith("https://www.wikidata.org/prop/P"):
            ent_preds.add(pred.replace("/prop/", "/entity/"))
    
    outfile_url = Path(outfile).parent / f"{Path(outfile).stem}.urls.txt"

    if filter_duplication and os.path.exists(outfile_url):
        # we always write these ent_preds to a separated file, so we can ensure uniqueness
        with open(outfile_url, "r") as f:
            existing_ent_preds = {line.strip() for line in f}
            n_ent_preds = len(ent_preds)
            ent_preds = ent_preds - existing_ent_preds
            print(">>> filter duplication from %s down to %s", n_ent_preds, len(ent_preds))
            
    if len(ent_preds) > 0:
        # write the predicate we are going to query
        ent_preds = list(ent_preds)
        with open(outfile_url, "a") as f:
            for e in ent_preds:
                f.write(e + "\n")
        # query them
        download_entity_properties(ent_preds, outfile, batch_size)


def read_entity(subj: str, g: rdflib.Graph, keep_props: Set[str]=set(), keep_langs: Set[str]=set(), ignore_props: Set[str]=set(), ignore_prop_patterns: list=[]):
    """
    Read all properties of an entity from a graph

    @param subj: the entity that we want to obtain full information
    @param g: the graph that contains triples of the entity, if it is Fuseki instance, we will query the triple store first
    @param keep_props: set of properties of predicates that we want to keep (e.g., skos:altLabel, rdfs:label). We keep all when it's empty
    @param keep_langs: set of languages of property's values  that we want to keep (e.g., en). Keep all when it's empty. If you want to also keep literals that don't have language tag, add None to the set
    @param ignore_props: set of properties that we will discard
    @param ignore_prop_patterns: set of regex patterns of properties that we will discard
    """
    odict = {
        '@id': subj
    }
    if not isinstance(g, rdflib.Graph):
        g = g.describe("DESCRIBE ?item WHERE { VALUES ?item { <%s> } }" % subj)

    for p, o in g.predicate_objects(rdflib.term.URIRef(subj)):
        str_p = str(p)
        if len(keep_props) > 0 and str_p not in keep_props:
            continue
        # remove the o.language null check, so that we have option to keep literals that don't have language tag
        if len(keep_langs) > 0 and isinstance(o, rdflib.term.Literal) and o.language not in keep_langs:
            continue

        if str_p in ignore_props:
            continue

        if any(ptn.match(str_p) for ptn in ignore_prop_patterns):
            continue

        if str_p not in odict:
            odict[str_p] = []
        odict[str_p].append(o)
    return odict


def read_batch_entities(subjs: List[str], g: rdflib.Graph, keep_props: Set[str]=set(), keep_langs: Set[str]=set(), ignore_props: Set[str]=set(), ignore_prop_patterns: list=[], return_type: str='list'):
    """
    A batch version of the `read_entity` function
    """
    if not isinstance(g, rdflib.Graph):
        g = g.describe("describe ?item where { values ?item { %s }}" % (
            " ".join((
                "<%s>" % subj for subj in subjs
            ))
        ))
    
    if return_type == 'list':
        return [read_entity(subj, g, keep_props, keep_langs, ignore_props, ignore_prop_patterns) for subj in subjs]
    else:
        return {
            subj: read_entity(subj, g, keep_props, keep_langs, ignore_props, ignore_prop_patterns)
            for subj in subjs
        }


def read_predicates(infile_or_g: str, keep_props: Set[str]=set(), keep_langs: Set[str]=set()) -> Dict[str, Dict[str, Any]]:
    """
    Read information about wikidata predicates that has been stored using `wikidata_download.download_predicates` function

    @param infile_or_g: the file that contains downloaded triples in `ntriples` format or an fuseki instance
    @param keep_props: set of properties of predicates that we want to keep (e.g., skos:altLabel, rdfs:label). We keep all when it's empty
    @param keep_langs: set of languages of property's values  that we want to keep (e.g., en). Keep all when it's empty.
    """
    predicates = {}

    if isinstance(infile_or_g, str):
        # parse graph and read predicates
        start = time.time()
        g = rdflib.Graph()
        g.parse(infile_or_g, format='ntriples')
        print(">>> read graph take %.5f seconds" % (time.time() - start))

        # set of predicates
        subjs = set()
        for s in g.subjects():
            if s.startswith("http://www.wikidata.org/entity/P") or s.startswith("https://www.wikidata.org/entity/P"):
                subjs.add(s)
        
        for subj in tqdm(subjs):
            predicates[subj.replace("/entity/", "/prop/")] = read_entity(subj, g, keep_props, keep_langs)
    else:
        g = infile_or_g
        res = g.query("""select distinct ?s where {
    ?s ?p ?o.
    FILTER (strStarts(STR(?s) , 'http://www.wikidata.org/entity/P'))
}""")['results']['bindings']
        subjs = []
        for r in res:
            subjs.append(r['s']['value'])

        batch_size = 100
        with tqdm(total=len(subjs)) as pbar:
            for i in range(0, len(subjs), batch_size):
                b_subjs = subjs[i:i+batch_size]
                ents = read_batch_entities(b_subjs, g, keep_props, keep_langs)
                for subj, ent in zip(b_subjs, ents):
                    predicates[subj.replace("/entity/", "/prop/")] = ent
                pbar.update(len(b_subjs))
    
    return predicates
