use crate::LqsError;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type MessageAttributes = BTreeMap<String, MessageAttribute>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAttribute {
    /// String, Number or Binary, optionally followed by a custom .suffix.
    pub data_type: String,
    pub value: MessageAttributeValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageAttributeValue {
    String(String),
    Binary(Vec<u8>),
}

impl MessageAttributeValue {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::String(value) => value.as_bytes(),
            Self::Binary(value) => value,
        }
    }
}

pub fn message_attributes_size(attributes: &MessageAttributes) -> usize {
    attributes.iter().fold(0usize, |sum, (name, attribute)| {
        sum.saturating_add(name.len())
            .saturating_add(attribute.data_type.len())
            .saturating_add(attribute.value.as_bytes().len())
    })
}

pub(crate) fn payload_size(body: &str, attributes: &MessageAttributes) -> usize {
    let raw = message_attributes_size(attributes);
    let mut normalized = attributes.clone();
    let normalized_size = if validate_message_attributes(&mut normalized).is_ok() {
        message_attributes_size(&normalized)
    } else {
        raw
    };
    body.len().saturating_add(raw.max(normalized_size))
}

pub fn message_attributes_md5(attributes: &MessageAttributes) -> Option<String> {
    if attributes.is_empty() {
        return None;
    }
    // BTreeMap provides ascending name order; lengths are unsigned 32-bit big endian.
    let mut digest = md5::Context::new();
    for (name, attribute) in attributes {
        for bytes in [name.as_bytes(), attribute.data_type.as_bytes()] {
            digest.consume((bytes.len() as u32).to_be_bytes());
            digest.consume(bytes);
        }
        digest.consume([match attribute.value {
            MessageAttributeValue::String(_) => 1u8,
            MessageAttributeValue::Binary(_) => 2u8,
        }]);
        let bytes = attribute.value.as_bytes();
        digest.consume((bytes.len() as u32).to_be_bytes());
        digest.consume(bytes);
    }
    Some(format!("{:x}", digest.finalize()))
}

pub(crate) fn valid_xml_text(value: &str) -> bool {
    value.chars().all(
        |c| matches!(c as u32, 9 | 10 | 13 | 0x20..=0xd7ff | 0xe000..=0xfffd | 0x10000..=0x10ffff),
    )
}

pub(crate) fn validate_message_attributes(
    attributes: &mut MessageAttributes,
) -> Result<(), LqsError> {
    let invalid = |reason: &str| LqsError::InvalidMessageAttributes(reason.to_owned());
    if attributes.len() > 10 {
        return Err(invalid("at most 10 attributes are allowed"));
    }
    for (name, attribute) in attributes {
        let lower = name.to_ascii_lowercase();
        if name.is_empty()
            || name.len() > 256
            || lower.starts_with("aws.")
            || lower.starts_with("amazon.")
            || name.starts_with('.')
            || name.ends_with('.')
            || name.contains("..")
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        {
            return Err(invalid("invalid attribute name"));
        }
        let (kind, suffix) = attribute
            .data_type
            .split_once('.')
            .map_or((attribute.data_type.as_str(), None), |(kind, suffix)| {
                (kind, Some(suffix))
            });
        if attribute.data_type.chars().count() > 256
            || !valid_xml_text(&attribute.data_type)
            || suffix == Some("")
        {
            return Err(invalid("invalid attribute data type"));
        }
        if attribute.value.as_bytes().is_empty() {
            return Err(invalid("attribute values must not be empty"));
        }
        match (kind, &mut attribute.value) {
            ("String", MessageAttributeValue::String(value)) if valid_xml_text(value) => {}
            ("Number", MessageAttributeValue::String(value)) => {
                *value = normalize_number(value).ok_or_else(|| invalid("Number must have at most 38 digits of precision and magnitude 1e-128..1e126 (or zero)"))?;
            }
            ("Binary", MessageAttributeValue::Binary(_)) => {}
            _ => return Err(invalid("attribute type and value do not match")),
        }
    }
    Ok(())
}

// Decimal validation without a floating-point conversion (which would lose 38-digit precision).
fn normalize_number(value: &str) -> Option<String> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (mantissa, exponent) = unsigned
        .split_once(['e', 'E'])
        .map_or(Some((unsigned, 0i32)), |(mantissa, exponent)| {
            Some((mantissa, exponent.parse::<i32>().ok()?))
        })?;
    if exponent.unsigned_abs() > 1000 {
        return None;
    }
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if integer.len() + fraction.len() == 0
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{integer}{fraction}");
    let leading_trimmed = digits.trim_start_matches('0');
    if leading_trimmed.is_empty() {
        return Some("0".into());
    }
    let coefficient = leading_trimmed.trim_end_matches('0');
    if coefficient.len() > 38 {
        return None;
    }
    let scale = exponent
        .checked_sub(i32::try_from(fraction.len()).ok()?)?
        .checked_add(i32::try_from(leading_trimmed.len() - coefficient.len()).ok()?)?;
    let magnitude = i32::try_from(coefficient.len()).ok()? - 1 + scale;
    if !(-128..=126).contains(&magnitude) || (magnitude == 126 && coefficient != "1") {
        return None;
    }
    let point = coefficient.len() as i32 + scale;
    let normalized = if point <= 0 {
        format!("0.{}{coefficient}", "0".repeat((-point) as usize))
    } else if point as usize >= coefficient.len() {
        format!(
            "{coefficient}{}",
            "0".repeat(point as usize - coefficient.len())
        )
    } else {
        format!(
            "{}.{}",
            &coefficient[..point as usize],
            &coefficient[point as usize..]
        )
    };
    Some(format!("{}{normalized}", if negative { "-" } else { "" }))
}
