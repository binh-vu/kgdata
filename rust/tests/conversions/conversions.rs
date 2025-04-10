use crate::read_lines;
use anyhow::Result;
use kgdata::conversions::WDEntity;

#[test]
fn parse_entity() -> Result<()> {
    let ent = serde_json::from_str::<WDEntity>(
        r#"{"id":"test","type":"item","label":{"lang2value":{},"lang":"en"},"description":{"lang2value":{},"lang":"en"},"aliases":{"lang2values":{},"lang":"en"},"props":{"P31":[]},"sitelinks":{}}"#,
    )?
    .0;
    assert_eq!(ent.id, "test");

    let lines = read_lines("wdentities.jl")?;
    let ent = serde_json::from_str::<WDEntity>(&lines[0])?.0;
    assert_eq!(ent.id, "P4274");

    for line in lines {
        assert!(
            serde_json::from_str::<WDEntity>(&line).is_ok(),
            "Error parsing line: {}",
            line
        );
    }
    Ok(())
}

#[test]
fn parse_multilingual() -> Result<()> {
    let val = r#"{"lang2value":{"en":"Tunisian geographic code","fr":"code géographique tunisien","ar":"الترميز الجغرافي التونسي","de":"tunesische Ortskennung","ru":"географический код Туниса","pt":"código geográfico tunisiano","mk":"туниски географски код","uk":"географічний код Тунісу","zh-hans":"突尼斯地区代码","es":"código geográfico tunesino","nl":"geografie Tunesië-identificatiecode","pl":"Tunezyjski kod geograficzny"},"lang":"en"}"#;
    let ml: kgdata::models::multilingual::MultiLingualString = serde_json::from_str(val)?;
    assert_eq!(ml.lang2value.len(), 12);
    Ok(())
}
