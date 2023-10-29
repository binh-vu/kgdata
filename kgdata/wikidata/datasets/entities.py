from functools import lru_cache, partial
from typing import Dict, Optional, Set, Union

import orjson
from loguru import logger
from pyspark import Broadcast
from sm.misc.funcs import filter_duplication, is_not_null

from kgdata.dataset import Dataset
from kgdata.misc.funcs import split_tab_2
from kgdata.spark import get_spark_context
from kgdata.wikidata.config import WikidataDirCfg
from kgdata.wikidata.datasets.entity_dump import entity_dump
from kgdata.wikidata.datasets.entity_ids import entity_ids
from kgdata.wikidata.datasets.entity_redirections import entity_redirections
from kgdata.wikidata.models.wdentity import WDEntity
from kgdata.wikidata.models.wdstatement import WDStatement


@lru_cache()
def entities(lang: str = "en") -> Dataset[WDEntity]:
    """Normalize Wikidata entity from Wikidata entity json dumps.

    In the json dumps, an entity can linked to an entity that either:
    1. in the dump
    2. not in the dump but in a redirection list
    3. not in the dump and not in a redirection list (e.g., deleted entity or lexeme that is not included in the dump).

    Similarly, property of an entity can also not in the dump.

    This dataset fixed all the issues mentioned above by resolving redirection and removed unknown entities (including properties).

    Returns:
        Dataset[WDEntity]
    """
    cfg = WikidataDirCfg.get_instance()

    dangling_id_ds = Dataset.string(
        cfg.entities / "dangling_ids" / "*.gz",
        name="entities/dangling-ids",
        dependencies=[entity_dump(), entity_ids()],
    )
    unk_id_ds = Dataset.string(
        cfg.entities / "unknown_ids/part-*",
        name="entities/unknown-ids",
        dependencies=[dangling_id_ds, entity_redirections()],
    )
    redirected_id_ds = Dataset(
        cfg.entities / "redirected_ids/part-*",
        deserialize=split_tab_2,
        name="entities/redirected-ids",
        dependencies=[dangling_id_ds, entity_redirections()],
    )
    invalid_qualifier_ds = Dataset(
        cfg.entities / "error_invalid_qualifiers/part-*",
        deserialize=deser_entity,
        name="entities/error-invalid-qualifiers",
        dependencies=[entity_dump()],
    )
    fixed_ds = Dataset(
        cfg.entities / lang / "*.gz",
        deserialize=deser_entity,
        name=f"entities/{lang}/fixed",
        dependencies=[entity_dump(), entity_ids(), entity_redirections()],
    )

    if not dangling_id_ds.has_complete_data():
        logger.info("Getting all entity ids in the dump (including properties)")
        (
            entity_dump()
            .get_extended_rdd()
            .map(partial(WDEntity.from_wikidump, lang="en"))
            .flatMap(get_child_entities)
            .distinct()
            .subtract(entity_ids().get_extended_rdd())
            .save_like_dataset(dangling_id_ds)
        )

    if not unk_id_ds.has_complete_data():
        logger.info("Identifying unknown entities")
        (
            dangling_id_ds.get_extended_rdd()
            .subtract(entity_redirections().get_extended_rdd().map(lambda x: x[0]))
            .save_like_dataset(unk_id_ds, auto_coalesce=True)
        )

    if not redirected_id_ds.has_complete_data():
        logger.info("Identifying redirected entities")
        (
            dangling_id_ds.get_extended_rdd()
            .map(lambda x: (x, 1))
            .join(entity_redirections().get_extended_rdd())
            .map(lambda x: (x[0], x[1][1]))
            .map(lambda x: "\t".join(x))
            .save_like_dataset(redirected_id_ds, auto_coalesce=True)
        )

    if not invalid_qualifier_ds.has_complete_data():
        (
            entity_dump()
            .get_extended_rdd()
            .map(partial(WDEntity.from_wikidump, lang=lang))
            .map(extract_invalid_qualifier)
            .filter_update_type(is_not_null)
            .map(ser_entity)
            .save_like_dataset(
                invalid_qualifier_ds, auto_coalesce=True, min_num_partitions=1
            )
        )

    need_verification = False

    if not fixed_ds.has_complete_data():
        logger.info("Normalizing and fixing the entities in the dump")

        sc = get_spark_context()
        unknown_entities = sc.broadcast(unk_id_ds.get_set())
        redirected_entities = sc.broadcast(redirected_id_ds.get_dict())

        (
            entity_dump()
            .get_extended_rdd()
            .map(partial(WDEntity.from_wikidump, lang=lang))
            .map(fix_transitive_qualifier)
            .map(lambda x: fixed_entity(x, unknown_entities, redirected_entities))
            .map(lambda x: x[0])
            .map(ser_entity)
            .save_like_dataset(
                fixed_ds,
                auto_coalesce=True,
                shuffle=True,
                trust_dataset_dependencies=True,
            )
        )
        need_verification = True

    if need_verification:
        logger.info("Verifying if entities contain unique ids")
        assert fixed_ds.get_extended_rdd().is_unique(lambda x: x.id)

        n_ents = fixed_ds.get_rdd().count()
        n_ents_from_dump = entity_dump().get_rdd().count()
        assert n_ents == n_ents_from_dump, f"{n_ents} != {n_ents_from_dump}"
        logger.info("The entity dataset is unique and has {} entities", n_ents)

    return fixed_ds


def deser_entity(line: Union[str, bytes]) -> WDEntity:
    return WDEntity.from_dict(orjson.loads(line))


def ser_entity(ent: WDEntity):
    return orjson.dumps(ent.to_dict())


def get_child_entities(ent: WDEntity):
    """Get entities that are children of an entity and properties/qualifiers used by the entity."""
    children: set[str] = set()
    for pid, stmts in ent.props.items():
        children.add(pid)
        for stmt in stmts:
            if stmt.value.is_entity_id(stmt.value):
                children.add(stmt.value.as_entity_id())

            for qid, qvals in stmt.qualifiers.items():
                for qval in qvals:
                    if qval.is_entity_id(qval):
                        children.add(qval.as_entity_id())
                children.add(qid)
    return list(children)


def fixed_entity(
    ent: WDEntity, bc_unknown_entities: Broadcast, bc_redirected_entities: Broadcast
):
    unknown_entities: Set[str] = bc_unknown_entities.value
    redirected_entities: Dict[str, str] = bc_redirected_entities.value

    is_fixed = False
    for pid, stmts in list(ent.props.items()):
        if pid in unknown_entities:
            del ent.props[pid]
            is_fixed = True
            continue

        removed_stmts = set()
        for i, stmt in enumerate(stmts):
            if stmt.value.is_entity_id(stmt.value):
                ent_id = stmt.value.as_entity_id()
                if ent_id in unknown_entities:
                    removed_stmts.add(i)
                    continue
                if ent_id in redirected_entities:
                    stmt.value.value["id"] = redirected_entities[ent_id]
                    is_fixed = True

            for qid, qvals in list(stmt.qualifiers.items()):
                if qid in unknown_entities:
                    del stmt.qualifiers[qid]
                    stmt.qualifiers_order = [
                        x for x in stmt.qualifiers_order if x != qid
                    ]
                    is_fixed = True
                    continue

                removed_qualifiers = set()
                for j, qval in enumerate(qvals):
                    if qval.is_entity_id(qval):
                        ent_id = qval.as_entity_id()
                        if ent_id in unknown_entities:
                            removed_qualifiers.add(j)
                            continue

                        if ent_id in redirected_entities:
                            qval.value["id"] = redirected_entities[ent_id]

                is_fixed = is_fixed or len(removed_qualifiers) > 0
                if len(removed_qualifiers) == len(qvals):
                    del stmt.qualifiers[qid]
                    stmt.qualifiers_order = [
                        x for x in stmt.qualifiers_order if x != qid
                    ]
                    continue

                if len(removed_qualifiers) > 0:
                    stmt.qualifiers[qid] = [
                        qval
                        for j, qval in enumerate(qvals)
                        if j not in removed_qualifiers
                    ]

                if qid in redirected_entities:
                    is_fixed = True
                    stmt.qualifiers[redirected_entities[qid]] = stmt.qualifiers.pop(qid)
                    stmt.qualifiers_order = [
                        x if x != qid else redirected_entities[qid]
                        for x in stmt.qualifiers_order
                    ]

        is_fixed = is_fixed or len(removed_stmts) > 0
        if len(removed_stmts) == len(stmts):
            del ent.props[pid]
            continue

        if len(removed_stmts) > 0:
            ent.props[pid] = [
                stmt for i, stmt in enumerate(stmts) if i not in removed_stmts
            ]

        if pid in redirected_entities:
            is_fixed = True
            ent.props[redirected_entities[pid]] = ent.props.pop(pid)

    return ent, is_fixed


def fix_transitive_qualifier(ent: WDEntity):
    """Fix transitive qualifiers that are the same as the property."""
    transitive_props = {"P276", "P131", "P527", "P2541"}

    for pid, stmts in ent.props.items():
        new_stmts: list[WDStatement] = []
        for stmt in stmts:
            for qid, qvals in list(stmt.qualifiers.items()):
                if qid != pid:
                    continue
                if qid in transitive_props:
                    for qval in qvals:
                        new_stmts.append(
                            WDStatement(
                                qval, qualifiers={}, qualifiers_order=[], rank=stmt.rank
                            )
                        )
                del stmt.qualifiers[qid]
                stmt.qualifiers_order = [x for x in stmt.qualifiers_order if x != qid]
        new_stmts = filter_duplication(
            new_stmts, key_fn=lambda x: x.value.to_string_repr()
        )
        stmts.extend(new_stmts)
    return ent


def extract_invalid_qualifier(ent: WDEntity) -> Optional[WDEntity]:
    newent = ent.shallow_clone()
    newent.props = {}
    for pid, stmts in ent.props.items():
        new_stmts: list[WDStatement] = []
        for stmt in stmts:
            for qid, qvals in list(stmt.qualifiers.items()):
                if qid != pid:
                    continue
                new_stmts.append(stmt)
        if len(new_stmts) > 0:
            newent.props[pid] = new_stmts

    if len(newent.props) > 0:
        return newent
    return None


if __name__ == "__main__":
    WikidataDirCfg.init("~/kgdata/wikidata/20211213")
    print("Total:", entities().get_rdd().count())
