from dataclasses import dataclass
from typing import List, Dict, Tuple, Union, Optional

import orjson


@dataclass
class DataValue:
    __slots__ = ('type', 'value')

    type: str
    # https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON#Data_Values
    value: Union[str, dict]

    def is_qnode(self):
        return self.type == 'wikibase-entityid'

    def is_string(self):
        return self.type == 'string'

    def is_time(self):
        return self.type == 'time'

    def is_quantity(self):
        return self.type == 'quantity'

    def is_mono_lingual_text(self):
        return self.type == 'monolingualtext'

    def is_globe_coordinate(self):
        return self.type == 'globecoordinate'

    def as_qnode_id(self):
        assert self.is_qnode()
        return self.value['id'].upper()

    def as_string(self):
        assert self.type == 'string'
        return self.value

    def as_qnode_label(self):
        assert self.is_qnode()
        """Label of the qnode doesn't come with the data. It is set using `set_qnode_label` function."""
        return self.value['label']

    def set_qnode_label(self, label):
        assert self.is_qnode()
        self.value['label'] = label

    def to_string_repr(self):
        if isinstance(self.value, str):
            return self.value
        return orjson.dumps(self.value).decode()
        # assert False, "Haven't handle the dict yet"


@dataclass
class SiteLink:
    __slots__ = ("site", "title", "badges", "url")

    site: str
    title: str
    badges: List[str]
    url: Optional[str]


@dataclass
class Statement:
    __slots__ = ("value", "qualifiers", "qualifiers_order")

    value: DataValue
    # mapping from qualifier id into data value
    qualifiers: Dict[str, List[DataValue]]
    # list of qualifiers id that records the order (as dict lacks of order)
    qualifiers_order: List[int]

    @staticmethod
    def from_dict(o):
        o['qualifiers'] = {
            k: [DataValue(**v) for v in vals]
            for k, vals in o['qualifiers'].items()
        }
        o['value'] = DataValue(**o['value'])
        return Statement(**o)


class MultiLingualString(str):
    def __new__(cls, lang2value: Dict[str, str], lang):
        object = str.__new__(cls, lang2value[lang])
        object.lang = lang
        object.lang2value = lang2value
        return object

    def as_lang(self, lang: str):
        return self.lang2value[lang]

    def serialize(self):
        return {"lang2value": self.lang2value, "lang": self.lang}

    def __getnewargs__(self) -> Tuple[str]:
        return (self.lang2value, self.lang)


class MultiLingualStringList(List[str]):
    def __init__(self, lang2values: Dict[str, List[str]], lang):
        super().__init__(lang2values[lang])
        self.lang2values = lang2values
        self.lang = lang

    def serialize(self):
        return {"lang2values": self.lang2values, "lang": self.lang}


@dataclass
class QNode:
    __slots__ = ("id", "type", "datatype", "label", "description", "aliases", "props", "sitelinks")

    id: str
    # possible values ["item", "property"]
    type: str
    label: MultiLingualString
    # the datatype is not described in the documentation: https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON
    # however, seems to appear for the property, and will tell us whether the property is an external identifier or not
    # possible values:
    # ['quantity', 'wikibase-form', 'geo-shape', 'math', 'wikibase-item', 'musical-notation', 'commonsMedia', 'wikibase-property', 'wikibase-lexeme', 'tabular-data', 'time', 'wikibase-sense', 'external-id', 'monolingualtext', 'string', 'globe-coordinate', 'url']
    datatype: Optional[str]
    description: MultiLingualString
    aliases: MultiLingualStringList
    props: Dict[str, List[Statement]]
    sitelinks: Dict[str, SiteLink]

    def serialize(self):
        # make it self, so we can serialize
        odict = {k: getattr(self, k) for k in
                 ['id', 'type', 'label', 'datatype', 'description', 'aliases', 'props', 'sitelinks']}
        for k in ['label', 'description', 'aliases']:
            odict[k] = odict[k].serialize()
        return orjson.dumps(odict,
                            option=orjson.OPT_SERIALIZE_DATACLASS,
                            default=list)

    @staticmethod
    def deserialize(s):
        o = orjson.loads(s)
        return QNode.from_dict(o)

    @staticmethod
    def from_dict(o):
        o['props'] = {
            k: [Statement.from_dict(v) for v in vals]
            for k, vals in o['props'].items()
        }
        o['sitelinks'] = {k: SiteLink(**v) for k, v in o['sitelinks'].items()}
        o['label'] = MultiLingualString(**o['label'])
        o['description'] = MultiLingualString(**o['description'])
        o['aliases'] = MultiLingualStringList(**o['aliases'])
        return QNode(**o)

    @staticmethod
    def from_wikidump(qnode, lang: str = 'en') -> 'QNode':
        """Extract essential information from qnode in the form that are easier to work with

        Read more about ranks and truthy statements:
            - https://www.wikidata.org/wiki/Help:Ranking
            - https://www.mediawiki.org/wiki/Wikibase/Indexing/RDF_Dump_Format#Truthy_statements

        Parameters
        ----------
        qnode : dict
            qnode
        lang : str, optional
            language, default is 'en'

        Returns
        -------
        QNode
        """
        props = {}
        sitelinks = {}

        for prop, stmts in qnode['claims'].items():
            prop_value = []
            for stmt in stmts:
                if stmt['rank'] == 'deprecated':
                    continue
                mainsnak = stmt['mainsnak']
                if mainsnak['snaktype'] != 'value':
                    assert 'datavalue' not in mainsnak
                    continue
                datavalue = mainsnak['datavalue']

                try:
                    value = datavalue['value']
                    type = datavalue['type']
                except:
                    print(datavalue)
                    raise

                qualifiers = {}
                if 'qualifiers' in stmt:
                    assert 'qualifiers-order' in stmt
                    for qualifier_id, qualifier_snaks in stmt['qualifiers'].items():
                        qualifier_values = []
                        for qualifier_snak in qualifier_snaks:
                            if qualifier_snak['snaktype'] != 'value':
                                assert 'datavalue' not in qualifier_snak['snaktype']
                                continue

                            qualifier_dvalue = qualifier_snak['datavalue']['value']
                            qualifier_dtype = qualifier_snak['datavalue']['type']

                            qualifier_values.append(DataValue(value=qualifier_dvalue, type=qualifier_dtype))
                        qualifiers[qualifier_id] = qualifier_values
                    qualifiers_order = stmt['qualifiers-order']
                else:
                    qualifiers_order = []

                prop_value.append(Statement(
                    value=DataValue(value=value, type=type),
                    qualifiers=qualifiers,
                    qualifiers_order=qualifiers_order
                ))
            props[prop] = prop_value

        for key, sitelink in qnode.get('sitelinks', {}).items():
            sitelinks[key] = SiteLink(sitelink['site'],
                                      sitelink['title'],
                                      badges=sitelink['badges'],
                                      url=sitelink.get('url', None))

        label = {k: v['value'] for k, v in qnode['labels'].items()}
        if lang not in label:
            label[lang] = ''
        label = MultiLingualString(label, lang)

        description = {k: v['value'] for k, v in qnode['descriptions'].items()}
        if lang not in description:
            description[lang] = ''
        description = MultiLingualString(description, lang)

        aliases = {
            k: [x['value'] for x in lst]
            for k, lst in qnode['aliases'].items()
        }
        if lang not in aliases:
            aliases[lang] = []
        aliases = MultiLingualStringList(aliases, lang)
        return QNode(
            id=qnode['id'].upper(),
            type=qnode['type'],
            datatype=qnode.get('datatype', None),
            label=label,
            description=description,
            aliases=aliases,
            props=props,
            sitelinks=sitelinks)
