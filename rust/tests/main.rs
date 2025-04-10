#[cfg(test)]
pub mod conversions;

use anyhow::Result;
use std::{fs, path::Path};

pub fn read_lines(filename: &str) -> Result<Vec<String>> {
    let filepath = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/resources")
        .join(filename);

    let content = fs::read_to_string(filepath)?;

    Ok(content
        .split("\n")
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned())
        .collect())
}
