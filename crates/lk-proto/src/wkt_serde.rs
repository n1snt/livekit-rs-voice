//! protojson-compliant serde implementations for the generated `google.protobuf`
//! well-known types. These are required because the Twirp/JSON wire contract
//! (as produced by Go's `protojson`) encodes `Timestamp`/`Duration` as strings,
//! `Any` as an object with an `@type` key and `Empty` as an empty object.

use std::fmt;

use serde::de::{Unexpected, Visitor};
use serde::{Deserializer, Serializer};

use crate::google::protobuf::{Any, Duration, Empty, Timestamp};

// ---------------------------------------------------------------------------
// Timestamp
// ---------------------------------------------------------------------------

impl serde::Serialize for Timestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_timestamp(self.seconds, self.nanos))
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TsVisitor;
        impl Visitor<'_> for TsVisitor {
            type Value = Timestamp;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "an RFC 3339 timestamp string")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Timestamp, E> {
                parse_timestamp(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(TsVisitor)
    }
}

fn format_timestamp(seconds: i64, nanos: i32) -> String {
    let (year, month, day) = civil_from_days(seconds.div_euclid(86_400));
    let secs_of_day = seconds.rem_euclid(86_400);
    let (hh, mm, ss) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );

    let mut out = format!("{year:04}-{month:02}-{day:02}T{hh:02}:{mm:02}:{ss:02}");
    if nanos != 0 {
        out.push_str(&format_fraction(nanos));
    }
    out.push('Z');
    out
}

/// Renders nanoseconds as a `.fraction` suffix using protojson's 3/6/9-digit
/// precision rule: 3 digits if exactly milliseconds, 6 if microseconds, else 9.
fn format_fraction(nanos: i32) -> String {
    let (digits, unit) = if nanos % 1_000_000 == 0 {
        (3usize, 1_000_000i32)
    } else if nanos % 1_000 == 0 {
        (6, 1_000)
    } else {
        (9, 1)
    };
    let frac = format!("{:0width$}", nanos / unit, width = digits);
    format!(".{frac}")
}

fn parse_timestamp(value: &str) -> Result<Timestamp, String> {
    let (date_part, rest) = value
        .split_once('T')
        .ok_or_else(|| format!("invalid timestamp {value:?}: missing 'T'"))?;
    let (time_part, offset) = split_offset(rest)?;

    let date: Vec<&str> = date_part.split('-').collect();
    if date.len() != 3 {
        return Err(format!("invalid timestamp {value:?}: bad date"));
    }
    let year: i64 = date[0]
        .parse()
        .map_err(|_| format!("invalid timestamp {value:?}: bad year"))?;
    let month: i64 = date[1]
        .parse()
        .map_err(|_| format!("invalid timestamp {value:?}: bad month"))?;
    let day: i64 = date[2]
        .parse()
        .map_err(|_| format!("invalid timestamp {value:?}: bad day"))?;

    let time: Vec<&str> = time_part.split(':').collect();
    if time.len() != 3 {
        return Err(format!("invalid timestamp {value:?}: bad time"));
    }
    let hh: i64 = time[0]
        .parse()
        .map_err(|_| format!("invalid timestamp {value:?}: bad hour"))?;
    let mm: i64 = time[1]
        .parse()
        .map_err(|_| format!("invalid timestamp {value:?}: bad minute"))?;
    let (ss_str, frac_nanos) = match time[2].split_once('.') {
        Some((secs, frac)) => {
            let nanos = parse_fraction(frac)?;
            (secs, nanos)
        }
        None => (time[2], 0),
    };
    let ss: i64 = ss_str
        .parse()
        .map_err(|_| format!("invalid timestamp {value:?}: bad second"))?;

    let (offset_sign, offset_seconds) = match offset {
        None => (1i64, 0i64),
        Some("Z") | Some("z") => (1, 0),
        Some(off) => {
            let (sign, off) = match off.as_bytes().first() {
                Some(b'+') => (1, &off[1..]),
                Some(b'-') => (-1, &off[1..]),
                _ => return Err(format!("invalid timestamp {value:?}: bad offset")),
            };
            let parts: Vec<&str> = off.split(':').collect();
            if parts.len() != 2 {
                return Err(format!("invalid timestamp {value:?}: bad offset"));
            }
            let oh: i64 = parts[0]
                .parse()
                .map_err(|_| format!("invalid timestamp {value:?}: bad offset"))?;
            let om: i64 = parts[1]
                .parse()
                .map_err(|_| format!("invalid timestamp {value:?}: bad offset"))?;
            (sign, oh * 3600 + om * 60)
        }
    };

    let days = days_from_civil(year, month, day);
    let total_seconds = days * 86_400 + hh * 3600 + mm * 60 + ss - offset_sign * offset_seconds;
    Ok(Timestamp {
        seconds: total_seconds,
        nanos: frac_nanos,
    })
}

fn split_offset(rest: &str) -> Result<(&str, Option<&str>), String> {
    if let Some(off) = rest.strip_suffix('Z').or_else(|| rest.strip_suffix('z')) {
        return Ok((off, Some("Z")));
    }
    // Find offset like +08:00 / -05:30 at the end.
    let bytes = rest.as_bytes();
    for i in 0..bytes.len() {
        if bytes[i] == b'+' || bytes[i] == b'-' {
            return Ok((&rest[..i], Some(&rest[i..])));
        }
    }
    Ok((rest, None))
}

fn parse_fraction(frac: &str) -> Result<i32, String> {
    if frac.is_empty() || frac.len() > 9 {
        return Err(format!("invalid fractional seconds {frac:?}"));
    }
    let mut digits = String::from(frac);
    while digits.len() < 9 {
        digits.push('0');
    }
    digits
        .parse::<i32>()
        .map_err(|_| format!("invalid fractional seconds {frac:?}"))
}

/// Days since 1970-01-01 from a civil date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9).rem_euclid(12);
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m as u32, d as u32)
}

// ---------------------------------------------------------------------------
// Duration
// ---------------------------------------------------------------------------

impl serde::Serialize for Duration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_duration(self.seconds, self.nanos))
    }
}

impl<'de> serde::Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DurVisitor;
        impl Visitor<'_> for DurVisitor {
            type Value = Duration;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a protobuf Duration string like \"3.500s\"")
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Duration, E> {
                parse_duration(value).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(DurVisitor)
    }
}

fn format_duration(seconds: i64, nanos: i32) -> String {
    if nanos == 0 {
        return format!("{seconds}s");
    }
    // nanos must be in (-1e9, 1e9) with the same sign as seconds for well-formed durations.
    let nanos = i64::from(nanos);
    let sign = if seconds < 0 || (seconds == 0 && nanos < 0) {
        "-"
    } else {
        ""
    };
    let secs = seconds.abs();
    let n = nanos.abs();
    // protojson: 3/6/9 digit precision, trailing zeros preserved at those boundaries.
    let frac = if n % 1_000_000 == 0 {
        format!("{:03}", n / 1_000_000)
    } else if n % 1_000 == 0 {
        format!("{:06}", n / 1_000)
    } else {
        format!("{:09}", n)
    };
    format!("{sign}{secs}.{frac}s")
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let (sign, rest) = match value.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, value),
    };
    let rest = rest
        .strip_suffix('s')
        .ok_or_else(|| format!("invalid duration {value:?}: missing 's' suffix"))?;
    let (secs_str, nanos) = match rest.split_once('.') {
        Some((secs, frac)) => (secs, parse_fraction(frac)?),
        None => (rest, 0),
    };
    let secs: i64 = secs_str
        .parse()
        .map_err(|_| format!("invalid duration {value:?}"))?;
    Ok(Duration {
        seconds: sign * secs,
        nanos: (sign * i64::from(nanos)) as i32,
    })
}

// ---------------------------------------------------------------------------
// Any
// ---------------------------------------------------------------------------

impl serde::Serialize for Any {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("@type", &self.type_url)?;
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for Any {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::Value::deserialize(deserializer)?
                .as_object()
                .cloned()
                .ok_or_else(|| serde::de::Error::custom("Any must be a JSON object"))?;
        let type_url = obj
            .get("@type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| serde::de::Error::custom("Any must contain an '@type' field"))?
            .to_string();
        Ok(Any {
            type_url,
            value: Vec::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Empty
// ---------------------------------------------------------------------------

impl serde::Serialize for Empty {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        serializer.serialize_map(Some(0))?.end()
    }
}

impl<'de> serde::Deserialize<'de> for Empty {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Null | serde_json::Value::Object(_) => Ok(Empty {}),
            other => Err(serde::de::Error::invalid_value(
                Unexpected::Other(&format!("{other}")),
                &"an empty object",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(seconds: i64, nanos: i32) -> Timestamp {
        Timestamp { seconds, nanos }
    }

    #[test]
    fn timestamp_round_trips() {
        let cases = [
            (ts(0, 0), "\"1970-01-01T00:00:00Z\""),
            (ts(1_700_000_000, 0), "\"2023-11-14T22:13:20Z\""),
            (
                ts(1_700_000_000, 123_000_000),
                "\"2023-11-14T22:13:20.123Z\"",
            ),
            (ts(-1, 0), "\"1969-12-31T23:59:59Z\""),
            (ts(1_700_000_000, 1), "\"2023-11-14T22:13:20.000000001Z\""),
        ];
        for (input, expected) in cases {
            assert_eq!(serde_json::to_string(&input).unwrap(), expected);
            let back: Timestamp = serde_json::from_str(expected).unwrap();
            assert_eq!(back, input);
        }
    }

    #[test]
    fn timestamp_parses_offsets() {
        let back: Timestamp = serde_json::from_str("\"2023-11-14T22:13:20.123Z\"").unwrap();
        assert_eq!(back, ts(1_700_000_000, 123_000_000));
        // +05:30 offset converts back to UTC
        let back: Timestamp = serde_json::from_str("\"2023-11-15T03:43:20.123+05:30\"").unwrap();
        assert_eq!(back, ts(1_700_000_000, 123_000_000));
    }

    #[test]
    fn duration_round_trips() {
        let cases = [
            (
                Duration {
                    seconds: 3,
                    nanos: 0,
                },
                "\"3s\"",
            ),
            (
                Duration {
                    seconds: 3,
                    nanos: 500_000_000,
                },
                "\"3.500s\"",
            ),
            (
                Duration {
                    seconds: 0,
                    nanos: 1,
                },
                "\"0.000000001s\"",
            ),
            (
                Duration {
                    seconds: -3,
                    nanos: -500_000_000,
                },
                "\"-3.500s\"",
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(serde_json::to_string(&input).unwrap(), expected);
            let back: Duration = serde_json::from_str(expected).unwrap();
            assert_eq!(back, input);
        }
    }

    #[test]
    fn any_and_empty() {
        let any = Any {
            type_url: "type.googleapis.com/livekit.Foo".to_string(),
            value: vec![1, 2, 3],
        };
        assert_eq!(
            serde_json::to_string(&any).unwrap(),
            r#"{"@type":"type.googleapis.com/livekit.Foo"}"#
        );
        let back: Any =
            serde_json::from_str(r#"{"@type":"type.googleapis.com/livekit.Foo"}"#).unwrap();
        assert_eq!(back.type_url, any.type_url);

        assert_eq!(serde_json::to_string(&Empty {}).unwrap(), "{}");
        let _: Empty = serde_json::from_str("{}").unwrap();
    }

    #[test]
    fn days_algorithm_known_values() {
        // 2023-11-14 == 19675 days since epoch
        assert_eq!(days_from_civil(2023, 11, 14), 19_675);
        assert_eq!(civil_from_days(19_675), (2023, 11, 14));
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(2000, 2, 29), 11_016); // leap year
    }
}
