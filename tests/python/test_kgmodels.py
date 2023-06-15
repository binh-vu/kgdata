from pathlib import Path

import orjson
import pytest

from kgdata.core.models import Entity


@pytest.fixture()
def ser_wdentities(resource_dir: Path):
    with open(resource_dir / "wdentities.jl", "rb") as f:
        return f.readlines()


def test_parse_entity(ser_wdentities):
    ents = [Entity.from_wdentity_json(line) for line in ser_wdentities]
    assert ents[0].id == "P4274"
    assert sorted(ents[0].props.keys()) == [
        "P1629",
        "P17",
        "P1793",
        "P1855",
        "P1896",
        "P2302",
        "P31",
        "P3254",
    ]
    assert (
        ents[0].props["P17"][0].value.to_string_repr()
        == '{"entity-id":{"id":"Q948","entity-type":"item","numeric-id":948}}'
    )
