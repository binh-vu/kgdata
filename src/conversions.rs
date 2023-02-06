use crate::models::entity::{Entity, Statement};
use crate::models::value::Value;
use hashbrown::HashMap;
use serde::de::{Deserialize, Deserializer, Error, MapAccess, SeqAccess, Visitor};
use std::fmt;

pub struct WDEntity(pub Entity);
pub struct WDStatement(pub Statement);
pub struct WDValue(pub Value);
pub struct WDEntityProps(HashMap<String, Vec<Statement>>);

pub struct WDStatementQualifiers(HashMap<String, Vec<Value>>);

pub struct VecWDStatement(Vec<Statement>);

pub struct VecWDValue(Vec<Value>);

impl<'de> Deserialize<'de> for WDEntity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WDEntityVisitor;

        impl<'de> Visitor<'de> for WDEntityVisitor {
            type Value = WDEntity;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON string contains a serialized WDEntity")
            }

            fn visit_map<M>(self, mut map: M) -> Result<WDEntity, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut id = None;
                let mut entity_type = None;
                let mut label = None;
                let mut description = None;
                let mut aliases = None;
                let mut sitelinks = None;
                let mut props: Option<WDEntityProps> = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "id" => {
                            id = map.next_value().map_err(|e| {
                                Error::custom(format!("deser entity's id: {}", e.to_string()))
                            })?;
                        }
                        "type" => {
                            entity_type = map.next_value().map_err(|e| {
                                Error::custom(format!("deser entity's type: {}", e.to_string()))
                            })?
                        }
                        "label" => {
                            label = map.next_value().map_err(|e| {
                                Error::custom(format!("deser entity's label: {}", e.to_string()))
                            })?;
                        }
                        "description" => {
                            description = map.next_value().map_err(|e| {
                                Error::custom(format!(
                                    "deser entity's description: {}",
                                    e.to_string()
                                ))
                            })?;
                        }
                        "aliases" => {
                            aliases = map.next_value().map_err(|e| {
                                Error::custom(format!("deser entity's aliases: {}", e.to_string()))
                            })?;
                        }
                        "sitelinks" => {
                            sitelinks = map.next_value().map_err(|e| {
                                Error::custom(format!(
                                    "deser entity's sitelinks: {}",
                                    e.to_string()
                                ))
                            })?;
                        }
                        "props" => {
                            props = map.next_value().map_err(|e| {
                                Error::custom(format!("deser entity's props: {}", e.to_string()))
                            })?;
                        }
                        "datatype" => {
                            let _datatype: Option<&str> = map.next_value().map_err(|e| {
                                Error::custom(format!("deser entity's datatype: {}", e.to_string()))
                            })?;
                        }
                        _ => {
                            return Err(M::Error::unknown_field(
                                key,
                                &[
                                    "id",
                                    "type",
                                    "label",
                                    "description",
                                    "aliases",
                                    "sitelinks",
                                    "props",
                                ],
                            ));
                        }
                    }
                }
                let id = id.ok_or_else(|| Error::missing_field("id"))?;
                let entity_type = entity_type.ok_or_else(|| Error::missing_field("type"))?;
                let label = label.ok_or_else(|| Error::missing_field("label"))?;
                let description = description.ok_or_else(|| Error::missing_field("description"))?;
                let aliases = aliases.ok_or_else(|| Error::missing_field("aliases"))?;
                let sitelinks = sitelinks.ok_or_else(|| Error::missing_field("sitelinks"))?;
                let props = props.ok_or_else(|| Error::missing_field("props"))?;
                Ok(WDEntity(Entity {
                    id,
                    entity_type,
                    label,
                    description,
                    aliases,
                    sitelinks,
                    props: props.0,
                }))
            }
        }

        deserializer.deserialize_map(WDEntityVisitor {})
    }
}

impl<'de> Deserialize<'de> for WDStatement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct WDStatementVisitor;

        impl<'de> Visitor<'de> for WDStatementVisitor {
            type Value = WDStatement;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON string contains a serialized WDStatement")
            }

            fn visit_map<M>(self, mut map: M) -> Result<WDStatement, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut value: Option<WDValue> = None;
                let mut qualifiers: Option<WDStatementQualifiers> = None;
                let mut qualifiers_order = None;
                let mut rank = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "value" => {
                            value = map.next_value().map_err(|e| {
                                Error::custom(format!("deser statement's value: {}", e.to_string()))
                            })?;
                        }
                        "qualifiers" => {
                            qualifiers = map.next_value().map_err(|e| {
                                Error::custom(format!(
                                    "deser statement's qualifiers: {}",
                                    e.to_string()
                                ))
                            })?;
                        }
                        "qualifiers_order" => {
                            qualifiers_order = map.next_value().map_err(|e| {
                                Error::custom(format!(
                                    "deser statement's qualifiers_order: {}",
                                    e.to_string()
                                ))
                            })?;
                        }
                        "rank" => {
                            rank = map.next_value().map_err(|e| {
                                Error::custom(format!("deser statement's rank: {}", e.to_string()))
                            })?;
                        }
                        _ => {
                            return Err(M::Error::unknown_field(
                                key,
                                &["value", "qualifiers", "qualifiers_order", "rank"],
                            ));
                        }
                    }
                }
                let value = value.ok_or_else(|| Error::missing_field("value"))?;
                let qualifiers = qualifiers.ok_or_else(|| Error::missing_field("qualifiers"))?;
                let qualifiers_order =
                    qualifiers_order.ok_or_else(|| Error::missing_field("qualifiers_order"))?;
                let rank = rank.ok_or_else(|| Error::missing_field("rank"))?;
                Ok(WDStatement(Statement {
                    value: value.0,
                    qualifiers: qualifiers.0,
                    qualifiers_order,
                    rank,
                }))
            }
        }

        deserializer.deserialize_map(WDStatementVisitor {})
    }
}

impl<'de> Deserialize<'de> for WDValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CustomVisitor;

        impl<'de> Visitor<'de> for CustomVisitor {
            type Value = WDValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON string contains a map")
            }

            fn visit_map<M>(self, mut map: M) -> Result<WDValue, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut key: &str;
                key = map.next_key()?.unwrap_or("");
                if key != "type" {
                    return Err(Error::missing_field("expect field `type` first."));
                }
                let vtype: String = map.next_value()?;
                key = map.next_key()?.unwrap_or("");
                if key != "value" {
                    return Err(Error::missing_field("expect field `value`"));
                }

                match vtype.as_str() {
                    "string" => Ok(WDValue(Value::String(map.next_value().map_err(|e| {
                        Error::custom(format!("deser value of type string: {}", e.to_string()))
                    })?))),
                    "wikibase-entityid" => {
                        Ok(WDValue(Value::EntityId(map.next_value().map_err(|e| {
                            Error::custom(format!(
                                "deser value of type wikibase-entityid: {}",
                                e.to_string()
                            ))
                        })?)))
                    }
                    "time" => Ok(WDValue(Value::Time(map.next_value().map_err(|e| {
                        Error::custom(format!("deser value of type time: {}", e.to_string()))
                    })?))),
                    "globecoordinate" => Ok(WDValue(Value::GlobeCoordinate(
                        map.next_value().map_err(|e| {
                            Error::custom(format!(
                                "deser value of type globecoordinate: {}",
                                e.to_string()
                            ))
                        })?,
                    ))),
                    "quantity" => Ok(WDValue(Value::Quantity(map.next_value().map_err(|e| {
                        Error::custom(format!("deser value of type quanitty: {}", e.to_string()))
                    })?))),
                    "monolingualtext" => Ok(WDValue(Value::MonolingualText(
                        map.next_value().map_err(|e| {
                            Error::custom(format!(
                                "deser value of type monolingualtext: {}",
                                e.to_string()
                            ))
                        })?,
                    ))),
                    _ => {
                        return Err(M::Error::unknown_field(
                            key,
                            &[
                                "string",
                                "wikibase-entityid",
                                "time",
                                "globecoordinate",
                                "quantity",
                                "monolingualtext",
                            ],
                        ));
                    }
                }
            }
        }

        deserializer.deserialize_map(CustomVisitor {})
    }
}

impl<'de> Deserialize<'de> for VecWDStatement {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct VecVisitor;

        impl<'de> Visitor<'de> for VecVisitor {
            type Value = VecWDStatement;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON string contains a sequence of WDStatement")
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<VecWDStatement, S::Error>
            where
                S: SeqAccess<'de>,
            {
                let mut vec: Vec<Statement> = Vec::new();
                while let Some(elem) = seq.next_element::<WDStatement>()? {
                    vec.push(elem.0);
                }
                Ok(VecWDStatement(vec))
            }
        }

        deserializer.deserialize_seq(VecVisitor {})
    }
}

impl<'de> Deserialize<'de> for VecWDValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CustomVisitor;

        impl<'de> Visitor<'de> for CustomVisitor {
            type Value = VecWDValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON string contains a sequence of WDValue")
            }

            fn visit_seq<S>(self, mut seq: S) -> Result<VecWDValue, S::Error>
            where
                S: SeqAccess<'de>,
            {
                let mut vec: Vec<Value> = Vec::new();
                while let Some(elem) = seq.next_element::<WDValue>()? {
                    vec.push(elem.0);
                }
                Ok(VecWDValue(vec))
            }
        }

        deserializer.deserialize_seq(CustomVisitor {})
    }
}

impl<'de> Deserialize<'de> for WDEntityProps {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CustomVisitor;

        impl<'de> Visitor<'de> for CustomVisitor {
            type Value = WDEntityProps;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "a JSON string contains a serialized dictionary of WDEntity's properties",
                )
            }

            fn visit_map<M>(self, mut map: M) -> Result<WDEntityProps, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut mapping = HashMap::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(key) = map.next_key()? {
                    let value: VecWDStatement = map.next_value()?;
                    mapping.insert(key, value.0);
                }
                Ok(WDEntityProps(mapping))
            }
        }

        deserializer.deserialize_map(CustomVisitor {})
    }
}

impl<'de> Deserialize<'de> for WDStatementQualifiers {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct CustomVisitor;

        impl<'de> Visitor<'de> for CustomVisitor {
            type Value = WDStatementQualifiers;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a JSON string contains a serialized WDEntity")
            }

            fn visit_map<M>(self, mut map: M) -> Result<WDStatementQualifiers, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut mapping = HashMap::with_capacity(map.size_hint().unwrap_or(0));
                while let Some(key) = map.next_key()? {
                    let value: VecWDValue = map.next_value()?;
                    mapping.insert(key, value.0);
                }
                Ok(WDStatementQualifiers(mapping))
            }
        }

        deserializer.deserialize_map(CustomVisitor {})
    }
}
