from typing import (
    Generic,
    ItemsView,
    Iterator,
    KeysView,
    Literal,
    Optional,
    TypeVar,
    ValuesView,
)

from kgdata.core.base import RustMapView, RustVecView

V = TypeVar("V")

EntityType = Literal["item", "property"]

class EntityView:
    id: str
    entity_type: EntityType
    label: MultiLingualStringView
    description: MultiLingualStringView
    aliases: MultiLingualStringListView
    sitelinks: RustMapView[str, SiteLinkView]
    props: RustMapView[str, RustVecView[StatementView]]

class Entity:
    id: str
    entity_type: EntityType
    label: MultiLingualStringView
    description: MultiLingualStringView
    aliases: MultiLingualStringListView
    sitelinks: RustMapView[str, SiteLinkView]
    props: RustMapView[str, RustVecView[StatementView]]

    @staticmethod
    def from_wdentity_json(data: bytes) -> Entity: ...

class EntityMetadata:
    id: str
    label: MultiLingualStringView
    description: MultiLingualStringView
    aliases: MultiLingualStringListView
    instanceof: RustVecView[str]
    subclassof: RustVecView[str]
    subpropertyof: RustVecView[str]

    def get_all_aliases(self) -> list[str]: ...

class Property:
    id: str
    label: MultiLingualStringView
    description: MultiLingualStringView
    aliases: MultiLingualStringListView
    datatype: str
    instanceof: RustVecView[str]
    parents: RustVecView[str]
    ancestors: RustMapView[str, int]
    inverse_properties: RustVecView[str]
    related_properties: RustVecView[str]
    equivalent_properties: RustVecView[str]
    domains: RustVecView[str]
    ranges: RustVecView[str]

    def is_object_property(self) -> bool: ...
    def is_data_property(self) -> bool: ...

class SiteLinkView:
    site: str
    title: str
    badges: RustVecView[str]
    url: Optional[str]

class StatementView:
    def value(self) -> ValueView: ...
    def qualifiers_keys(self) -> KeysView[str]: ...
    def qualifiers_values(self) -> ValuesView[ListView[ValueView]]: ...
    def qualifiers_items(self) -> ItemsView[str, ListView[ValueView]]: ...
    def qualifiers_order(self) -> ListView[str]: ...
    def qualifier(self, id: str) -> ListView[ValueView]: ...
    def rank(self) -> Literal["normal", "preferred", "deprecated"]: ...

class ValueView:
    def get_type(
        self,
    ) -> Literal[
        "string", "entity-id", "quantity", "time", "globe-coordinate", "monolingualtext"
    ]: ...
    def is_str(self) -> bool: ...
    def is_entity_id(self) -> bool: ...
    def is_quantity(self) -> bool: ...
    def is_time(self) -> bool: ...
    def is_globe_coordinate(self) -> bool: ...
    def is_monolingual_text(self) -> bool: ...
    def as_str(self) -> str: ...
    def as_entity_id_str(self) -> str: ...
    def as_entity_id(self) -> EntityId: ...
    def as_quantity(self) -> Quantity: ...
    def as_time(self) -> Time: ...
    def as_globe_coordinate(self) -> GlobeCoordinate: ...
    def as_monolingual_text(self) -> MonolingualText: ...
    def to_string_repr(self) -> str: ...

class Value:
    @staticmethod
    def string(value: str) -> Value: ...
    @staticmethod
    def entity_id(
        id: str, entity_type: EntityType, numeric_id: Optional[int]
    ) -> Value: ...
    @staticmethod
    def time(
        time: str,
        timezone: int,
        before: int,
        after: int,
        precision: int,
        calendarmodel: str,
    ) -> Value: ...
    @staticmethod
    def quantity(
        amount: str,
        lower_bound: Optional[str],
        upper_bound: Optional[str],
        unit: str,
    ) -> Value: ...
    @staticmethod
    def globe_coordinate(
        latitude: float,
        longitude: float,
        precision: Optional[float],
        altitude: Optional[float],
        globe: str,
    ) -> Value: ...
    @staticmethod
    def monolingual_text(text: str, language: str) -> Value: ...
    def get_type(
        self,
    ) -> Literal[
        "string", "entity-id", "quantity", "time", "globe-coordinate", "monolingualtext"
    ]: ...
    def is_str(self) -> bool: ...
    def is_entity_id(self) -> bool: ...
    def is_quantity(self) -> bool: ...
    def is_time(self) -> bool: ...
    def is_globe_coordinate(self) -> bool: ...
    def is_monolingual_text(self) -> bool: ...
    def as_str(self) -> str: ...
    def as_entity_id_str(self) -> str: ...
    def as_entity_id(self) -> EntityId: ...
    def as_quantity(self) -> Quantity: ...
    def as_time(self) -> Time: ...
    def as_globe_coordinate(self) -> GlobeCoordinate: ...
    def as_monolingual_text(self) -> MonolingualText: ...
    def to_string_repr(self) -> str: ...

class EntityId:
    id: str
    entity_type: Literal["item", "property"]
    numeric_id: Optional[int]

class Time:
    time: str
    timezone: int
    before: int
    after: int
    precision: int
    calendarmodel: str

class Quantity:
    amount: str
    lower_bound: Optional[str]
    upper_bound: Optional[str]
    unit: str

class GlobeCoordinate:
    latitude: float
    longitude: float
    precision: float
    altitude: Optional[float]
    globe: str

class MonolingualText:
    text: str
    language: str

class MultiLingualStringView:
    default_lang: str  # default language

    def as_lang_default(self) -> str: ...
    def as_lang(self, lang: str) -> str: ...
    def to_list(self) -> list[str]: ...

class MultiLingualStringListView:
    default_lang: str  # default language

    def as_lang_default(self) -> list[str]: ...
    def as_lang(self, lang: str) -> list[str]: ...
    def flatten(self) -> list[str]: ...

class ListView(Iterator[V], Generic[V]):
    """A convenience class that is an iterator with random access. Note that when the iterator
    is exhausted, you won't be able to iterate over it again.
    For example:
    >>> l = ListView([1, 2, 3])  # demonstrate only as you won't be able to initialize it from Python
    >>> l[0]
    1
    >>> list(l)
    [1, 2, 3]
    >>> list(l)
    []
    >>> l[0]
    1
    """

    def __len__(self) -> int: ...
    def __getitem__(self, index: int) -> V: ...
