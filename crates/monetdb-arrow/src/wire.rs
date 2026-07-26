use chrono::{NaiveDate, TimeDelta};
use monetdb::MonetType;

pub(crate) fn fixed_wire_width(data_type: MonetType) -> Option<usize> {
    match data_type {
        MonetType::Bool | MonetType::TinyInt => Some(1),
        MonetType::SmallInt => Some(2),
        MonetType::Int | MonetType::Real | MonetType::MonthInterval | MonetType::Date => Some(4),
        MonetType::BigInt
        | MonetType::Oid
        | MonetType::Double
        | MonetType::DayInterval
        | MonetType::SecInterval
        | MonetType::Time
        | MonetType::TimeTz => Some(8),
        MonetType::Timestamp | MonetType::TimestampTz => Some(12),
        MonetType::HugeInt | MonetType::Uuid => Some(16),
        MonetType::Decimal(precision, _) => match precision {
            1..=2 => Some(1),
            3..=4 => Some(2),
            5..=9 => Some(4),
            10..=18 => Some(8),
            19..=38 => Some(16),
            _ => None,
        },
        MonetType::Varchar(_)
        | MonetType::Blob
        | MonetType::Url
        | MonetType::Inet
        | MonetType::Inet4
        | MonetType::Inet6
        | MonetType::Json
        | MonetType::Geometry
        | MonetType::Xml => None,
    }
}

pub fn date_from_unix_days(days: i64) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(1970, 1, 1)?.checked_add_signed(TimeDelta::try_days(days)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_widths_cover_wire_types() {
        let cases = [
            (MonetType::Bool, Some(1)),
            (MonetType::TinyInt, Some(1)),
            (MonetType::SmallInt, Some(2)),
            (MonetType::Int, Some(4)),
            (MonetType::Date, Some(4)),
            (MonetType::BigInt, Some(8)),
            (MonetType::TimeTz, Some(8)),
            (MonetType::Timestamp, Some(12)),
            (MonetType::Uuid, Some(16)),
            (MonetType::Decimal(2, 0), Some(1)),
            (MonetType::Decimal(4, 0), Some(2)),
            (MonetType::Decimal(9, 0), Some(4)),
            (MonetType::Decimal(18, 0), Some(8)),
            (MonetType::Decimal(38, 0), Some(16)),
            (MonetType::Decimal(0, 0), None),
            (MonetType::Varchar(0), None),
            (MonetType::Blob, None),
        ];
        for (data_type, expected) in cases {
            assert_eq!(fixed_wire_width(data_type), expected, "{data_type:?}");
        }
    }

    #[test]
    fn unix_day_conversion_is_checked() {
        assert_eq!(date_from_unix_days(0), NaiveDate::from_ymd_opt(1970, 1, 1));
        assert!(date_from_unix_days(i64::MAX).is_none());
    }
}
