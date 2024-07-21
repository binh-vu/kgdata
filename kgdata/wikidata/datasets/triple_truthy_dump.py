from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence, cast

import orjson
from kgdata.dataset import Dataset
from kgdata.dbpedia.datasets.ontology_dump import aggregated_triples
from kgdata.misc.ntriples_parser import (
    Triple,
    ignore_comment,
    node_from_dict,
    node_to_dict,
    ntriple_loads,
)
from kgdata.misc.resource import RDFResource
from kgdata.spark import ExtendedRDD
from kgdata.spark.common import does_result_dir_exist
from kgdata.splitter import split_a_file
from kgdata.wikidata.config import WikidataDirCfg
from rdflib import BNode, URIRef


@dataclass
class WikidataRDFResource(RDFResource):
    bnodes: dict[str, RDFResource]

    @staticmethod
    def deser(s: str | bytes):
        o = orjson.loads(s)
        return WikidataRDFResource(
            id=o["id"],
            props={k: [node_from_dict(v) for v in vs] for k, vs in o["props"].items()},
            bnodes={k: RDFResource.from_dict(v) for k, v in o["bnodes"].items()},
        )

    def to_dict(self):
        return {
            "id": self.id,
            "props": {k: [node_to_dict(v) for v in vs] for k, vs in self.props.items()},
            "bnodes": {k: v.to_dict() for k, v in self.bnodes.items()},
        }


def triple_truthy_dump() -> Dataset[RDFResource]:
    """Splitting nt truthy dumps of Wikidata from https://zenodo.org/records/7829583"""

    cfg = WikidataDirCfg.get_instance()
    dump_date = cfg.get_dump_date()

    split_dump_dir = cfg.triple_truthy_dump / "raw"
    final_dump_dir = cfg.triple_truthy_dump / "final"
    ds = Dataset(
        file_pattern=final_dump_dir / "*.zst",
        # deserialize=WikidataRDFResource.deser,
        deserialize=RDFResource.deser,
        name=f"triple-truthy-dump/{dump_date}",
        dependencies=[],
    )

    if not ds.has_complete_data():
        if not does_result_dir_exist(split_dump_dir, allow_override=False):
            raise Exception(
                f"{split_dump_dir} does not exists. "
                "Manually splitting truthy dump is much faster: `lbzip2 -cd ../../000_dumps/wikidata-20220521-truthy.nt.bz2 | split -d -l1000000 --suffix-length 5 --filter 'zstd -q -6 -o $FILE.zst' - part-"
            )
            split_a_file(
                infile=cfg.get_triple_truthy_dump_file(),
                outfile=split_dump_dir / "part-*.zst",
                n_writers=16,
                override=False,
                compression_level=9,
            )

        # (
        #     ExtendedRDD.textFile(split_dump_dir / "*.zst")
        #     .filter(ignore_comment)
        #     .map(ntriple_loads)
        #     .flatMap(reverse_bnode_triple)
        #     .groupBy(lambda x: x[0])
        #     .flatMap(inverse_bnode_triple)
        #     .groupBy(lambda x: x[0])
        #     .map(aggregated_wikidata_triples)
        #     .map(WikidataRDFResource.ser)
        #     .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        # )

        (
            ExtendedRDD.textFile(split_dump_dir / "*.zst")
            .filter(ignore_comment)
            .map(ntriple_loads)
            .groupBy(lambda x: x[0])
            .map(aggregated_triples)
            .map(RDFResource.ser)
            .save_like_dataset(ds, auto_coalesce=True, shuffle=True)
        )
    return ds


def reverse_bnode_triple(triple: Triple) -> Sequence[Triple | tuple[BNode, URIRef]]:
    if isinstance(triple[2], BNode):
        # don't expect to have two levels of bnodes
        assert isinstance(triple[0], URIRef), triple
        return [triple, (triple[2], triple[0])]
    return [triple]


def inverse_bnode_triple(
    id_triples: tuple[URIRef | BNode, Iterable[Triple | tuple[BNode, URIRef]]]
) -> list[tuple[str, int, RDFResource]]:
    id, triples = id_triples
    if isinstance(id, BNode) and any(len(triple) == 2 for triple in triples):
        bnode = RDFResource.from_triples(
            id, [triple for triple in triples if len(triple) == 3]
        )
        out = []
        for triple in triples:
            if len(triple) == 2:
                _, subj = triple
                out.append((str(subj), 1, bnode))
        return out
    else:
        resource = RDFResource.from_triples(id, cast(Iterable[Triple], triples))
        return [(str(id), 0, resource)]


def aggregated_wikidata_triples(
    id_resources: tuple[str, Iterable[tuple[str, int, RDFResource]]]
) -> WikidataRDFResource:
    id, resources = id_resources

    (main_resource,) = [r for subj, flag, r in resources if flag == 0]
    bnodes = {r.id: r for subj, flag, r in resources if flag == 1}
    return WikidataRDFResource(id=str(id), props=main_resource.props, bnodes=bnodes)
