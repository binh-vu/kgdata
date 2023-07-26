import gc
from pathlib import Path

from kgdata.wikidata.db import get_class_db, get_entity_db, get_prop_db
from sm.misc.funcs import assert_not_null


def test_get_entity_db(tmp_path: Path):
    db = get_entity_db(tmp_path / "entities.db", proxy=True)

    assert db["Q513"].label == "Mount Everest"
    assert assert_not_null(db.get("Q513")).label == "Mount Everest"

    del db
    gc.collect()

    db = get_entity_db(tmp_path / "entities.db", proxy=False)
    assert db["Q513"].label == "Mount Everest"


def test_get_wdclass_db(tmp_path: Path):
    db = get_class_db(tmp_path / "classes.db", proxy=True)

    assert db["Q8502"].label == "mountain"
    assert assert_not_null(db.get("Q8502")).label == "mountain"

    del db
    gc.collect()

    db = get_class_db(tmp_path / "classes.db", proxy=False)
    assert db["Q8502"].label == "mountain"


def test_get_wdprop_db(tmp_path: Path):
    db = get_prop_db(tmp_path / "props.db", proxy=True)

    assert db["P131"].label == "located in the administrative territorial entity"
    assert (
        assert_not_null(db.get("P131")).label
        == "located in the administrative territorial entity"
    )

    del db
    gc.collect()

    db = get_prop_db(tmp_path / "props.db", proxy=False)
    assert db["P131"].label == "located in the administrative territorial entity"
