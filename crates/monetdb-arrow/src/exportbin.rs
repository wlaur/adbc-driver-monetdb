//! Parsing of the `Xexportbin` binary result-set frame.
//!
//! Frame layout, per MonetDB's `documentation/source/binary-resultset.rst` and
//! the server implementation (`mvc_export_bin_chunk` in
//! `sql/backends/monet5/sql_result.c`):
//!
//! ```text
//! &6 <res_id> <nr_cols> <rows> <offset>\n        text header (Q_BLOCK)
//! <column 0 bytes>                               each column 32-byte aligned
//! ...
//! <column N-1 bytes>
//! <toc: nr_cols x (i64 start, i64 length)>       offsets from frame start
//! <i64 toc_pos>                                  negative => error in frame
//! ```
//!
//! The TOC integers and the trailing `toc_pos` are written in the byte order
//! the client requested at login (`mnstr_writeLng` in the implementation),
//! despite `binary-resultset.rst` describing them as server-endian. This driver
//! always requests little-endian.
//! Column *data* is written in the server's native byte order; only
//! little-endian servers are supported, so the whole frame is parsed as LE.

use std::fmt;
use std::ops::Range;

/// A parsed `Xexportbin` frame, borrowing the raw response buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportbinFrame<'a> {
    /// Server-side result-set id this window belongs to.
    pub result_id: i64,
    /// Number of rows in this window.
    pub row_count: u64,
    /// Absolute row offset of the window within the result set.
    pub start_row: u64,
    /// Raw bytes of each column, in result-set column order.
    pub columns: Vec<&'a [u8]>,
}

/// Header fields from an `Xexportbin` frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportbinHeader {
    /// Server-side result-set id this window belongs to.
    pub result_id: i64,
    /// Number of columns in the frame.
    pub column_count: usize,
    /// Number of rows in this window.
    pub row_count: u64,
    /// Absolute row offset of the window within the result set.
    pub start_row: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// The server reported an error instead of (or inside) a frame.
    Server(String),
    /// The response does not follow the documented frame layout.
    Malformed(&'static str),
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::Server(msg) => write!(f, "server error: {msg}"),
            FrameError::Malformed(what) => write!(f, "malformed Xexportbin frame: {what}"),
        }
    }
}

impl std::error::Error for FrameError {}

const HEADER_PREFIX: &[u8] = b"&6 ";
const TOC_ENTRY_SIZE: usize = 16;

/// Parse a complete `Xexportbin` response into column slices.
pub fn parse_frame(frame: &[u8]) -> Result<ExportbinFrame<'_>, FrameError> {
    if frame.first() == Some(&b'!') {
        return Err(FrameError::Server(read_error_message(frame, 0)));
    }
    if frame.len() < HEADER_PREFIX.len() + 8 {
        return Err(FrameError::Malformed("response too short"));
    }

    let (header, body_start) = parse_header(frame)?;

    let toc_pos = i64::from_le_bytes(frame[frame.len() - 8..].try_into().expect("8 bytes"));
    if toc_pos < 0 {
        return Err(FrameError::Server(find_error_message(frame, toc_pos)?));
    }
    let toc_pos = usize::try_from(toc_pos)
        .map_err(|_| FrameError::Malformed("table of contents offset does not fit in memory"))?;
    let toc_len = header
        .column_count
        .checked_mul(TOC_ENTRY_SIZE)
        .ok_or(FrameError::Malformed("table of contents length overflows"))?;
    let expected_end = toc_pos
        .checked_add(toc_len)
        .and_then(|end| end.checked_add(8));
    if header.column_count > frame.len() / TOC_ENTRY_SIZE
        || toc_pos < body_start
        || expected_end != Some(frame.len())
    {
        return Err(FrameError::Malformed("table of contents out of bounds"));
    }

    let mut ranges = Vec::with_capacity(header.column_count);
    for entry in frame[toc_pos..toc_pos + toc_len]
        .as_chunks::<TOC_ENTRY_SIZE>()
        .0
    {
        let start = i64::from_le_bytes(entry[..8].try_into().expect("8 bytes"));
        let length = i64::from_le_bytes(entry[8..].try_into().expect("8 bytes"));
        ranges.push(column_range(start, length, body_start, toc_pos)?);
    }
    let mut occupied = ranges
        .iter()
        .filter(|range| !range.is_empty())
        .collect::<Vec<_>>();
    occupied.sort_unstable_by_key(|range| range.start);
    if occupied.windows(2).any(|pair| pair[0].end > pair[1].start) {
        return Err(FrameError::Malformed("column ranges overlap"));
    }
    let columns = ranges.into_iter().map(|range| &frame[range]).collect();

    Ok(ExportbinFrame {
        result_id: header.result_id,
        row_count: header.row_count,
        start_row: header.start_row,
        columns,
    })
}

/// Parse only the text header of a complete `Xexportbin` response.
///
/// This is useful for scheduling row windows. Consumers must still call
/// [`parse_frame`] before accessing column bytes.
pub fn parse_frame_header(frame: &[u8]) -> Result<ExportbinHeader, FrameError> {
    if frame.first() == Some(&b'!') {
        return Err(FrameError::Server(read_error_message(frame, 0)));
    }
    parse_header(frame).map(|(header, _)| header)
}

/// Parse the `&6 <res_id> <nr_cols> <rows> <offset>\n` header line; returns the
/// header and the offset of the first byte after it.
fn parse_header(frame: &[u8]) -> Result<(ExportbinHeader, usize), FrameError> {
    if !frame.starts_with(HEADER_PREFIX) {
        return Err(FrameError::Malformed("missing &6 block header"));
    }
    // the header is short; a missing newline in the first 128 bytes is corruption
    let line_end = frame[..frame.len().min(128)]
        .iter()
        .position(|&b| b == b'\n')
        .ok_or(FrameError::Malformed("unterminated block header"))?;
    let line = str::from_utf8(&frame[HEADER_PREFIX.len()..line_end])
        .map_err(|_| FrameError::Malformed("block header is not UTF-8"))?;

    let mut fields = line.split_ascii_whitespace();
    let mut next = || {
        fields
            .next()
            .ok_or(FrameError::Malformed("block header has too few fields"))
    };
    let result_id = parse_int::<i64>(next()?)?;
    let column_count = parse_int::<usize>(next()?)?;
    let row_count = parse_int::<u64>(next()?)?;
    let start_row = parse_int::<u64>(next()?)?;

    Ok((
        ExportbinHeader {
            result_id,
            column_count,
            row_count,
            start_row,
        },
        line_end + 1,
    ))
}

fn parse_int<T: std::str::FromStr>(field: &str) -> Result<T, FrameError> {
    field
        .parse()
        .map_err(|_| FrameError::Malformed("block header field is not a number"))
}

fn column_range(
    start: i64,
    length: i64,
    body_start: usize,
    toc_pos: usize,
) -> Result<Range<usize>, FrameError> {
    let (Ok(start), Ok(length)) = (usize::try_from(start), usize::try_from(length)) else {
        return Err(FrameError::Malformed("negative column offset"));
    };
    let end = start
        .checked_add(length)
        .ok_or(FrameError::Malformed("column extent overflows"))?;
    if start < body_start || end > toc_pos {
        return Err(FrameError::Malformed("column data out of bounds"));
    }
    Ok(start..end)
}

/// Locate the NUL-terminated error message a negative `toc_pos` points at.
///
/// `documentation/source/binary-resultset.rst` defines the negated value as
/// the byte offset from the start of the response.
fn find_error_message(frame: &[u8], toc_pos: i64) -> Result<String, FrameError> {
    let message_end = frame.len() - 8;
    let Some(offset) = toc_pos
        .checked_neg()
        .and_then(|offset| usize::try_from(offset).ok())
        .filter(|&offset| offset < message_end && frame.get(offset) == Some(&b'!'))
    else {
        return Err(FrameError::Malformed("invalid in-frame error offset"));
    };
    Ok(read_error_message(&frame[..message_end], offset))
}

/// Read a `!`-prefixed, NUL-terminated error message starting at `offset`.
fn read_error_message(frame: &[u8], offset: usize) -> String {
    let message = &frame[offset..];
    let end = message
        .iter()
        .position(|&b| b == 0)
        .unwrap_or(message.len());
    let message = message[..end].strip_prefix(b"!").unwrap_or(&message[..end]);
    String::from_utf8_lossy(message).into_owned()
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// Build a syntactically valid frame the way `mvc_export_bin_chunk` does.
    fn build_frame(result_id: i64, start_row: u64, columns: &[&[u8]]) -> Vec<u8> {
        let row_count = 3; // arbitrary for framing purposes
        let mut frame =
            format!("&6 {result_id} {} {row_count} {start_row}\n", columns.len()).into_bytes();
        let mut toc = Vec::new();
        for column in columns {
            while frame.len() % 32 != 0 {
                frame.push(0);
            }
            toc.push((frame.len() as i64, column.len() as i64));
            frame.extend_from_slice(column);
        }
        while frame.len() % 32 != 0 {
            frame.push(0);
        }
        let toc_pos = frame.len() as i64;
        for (start, length) in toc {
            frame.extend_from_slice(&start.to_le_bytes());
            frame.extend_from_slice(&length.to_le_bytes());
        }
        frame.extend_from_slice(&toc_pos.to_le_bytes());
        frame
    }

    #[test]
    fn parses_two_column_frame() {
        let ints = 42i32.to_le_bytes();
        let strings = b"foo\0\x80\0bar\0";
        let frame = build_frame(7, 100, &[&ints, strings]);

        let parsed = parse_frame(&frame).unwrap();
        assert_eq!(parsed.result_id, 7);
        assert_eq!(parsed.row_count, 3);
        assert_eq!(parsed.start_row, 100);
        assert_eq!(parsed.columns, vec![&ints[..], &strings[..]]);
    }

    #[test]
    fn parses_header_without_walking_the_table_of_contents() {
        let frame = build_frame(7, 100, &[b"first", b"second"]);

        assert_eq!(
            parse_frame_header(&frame),
            Ok(ExportbinHeader {
                result_id: 7,
                column_count: 2,
                row_count: 3,
                start_row: 100,
            })
        );
    }

    #[test]
    fn header_parser_reports_textual_server_errors() {
        assert_eq!(
            parse_frame_header(b"!42000 syntax error\0"),
            Err(FrameError::Server("42000 syntax error".into()))
        );
    }

    #[test]
    fn parses_empty_columns() {
        let frame = build_frame(1, 0, &[&[], &[]]);
        let parsed = parse_frame(&frame).unwrap();
        assert_eq!(parsed.columns, vec![&[] as &[u8], &[]]);
    }

    #[test]
    fn reports_textual_server_error() {
        let err = parse_frame(b"!42000 syntax error\0").unwrap_err();
        assert_eq!(err, FrameError::Server("42000 syntax error".into()));
    }

    #[test]
    fn reports_in_frame_server_error() {
        let mut frame = b"&6 1 1 0 0\n".to_vec();
        let error_offset = frame.len() as i64;
        frame.extend_from_slice(b"!wrong\0");
        frame.extend_from_slice(&(-error_offset).to_le_bytes());

        let err = parse_frame(&frame).unwrap_err();
        assert_eq!(err, FrameError::Server("wrong".into()));
    }

    #[test]
    fn excludes_the_error_offset_from_unterminated_server_text() {
        let mut frame = b"&6 1 1 0 0\n".to_vec();
        let error_offset = frame.len() as i64;
        frame.extend_from_slice(b"!wrong");
        frame.extend_from_slice(&(-error_offset).to_le_bytes());

        let err = parse_frame(&frame).unwrap_err();
        assert_eq!(err, FrameError::Server("wrong".into()));
    }

    #[test]
    fn rejects_in_frame_error_with_invalid_offset() {
        let mut frame = b"&6 1 1 0 0\n!wrong\0".to_vec();
        frame.extend_from_slice(&(-1i64).to_le_bytes());

        let err = parse_frame(&frame).unwrap_err();
        assert_eq!(err, FrameError::Malformed("invalid in-frame error offset"));
    }

    #[test]
    fn rejects_in_frame_error_offset_into_trailer() {
        let error_offset = 223i64;
        let mut frame = b"&6 1 1 0 0\n".to_vec();
        frame.resize(error_offset as usize, 0);
        frame.extend_from_slice(&(-error_offset).to_le_bytes());
        assert_eq!(frame[error_offset as usize], b'!');

        let err = parse_frame(&frame).unwrap_err();
        assert_eq!(err, FrameError::Malformed("invalid in-frame error offset"));
    }

    #[test]
    fn rejects_truncated_frame() {
        let frame = build_frame(1, 0, &[b"data"]);
        let err = parse_frame(&frame[..frame.len() - 4]).unwrap_err();
        assert!(matches!(err, FrameError::Malformed(_)));
    }

    #[test]
    fn rejects_out_of_bounds_toc() {
        let mut frame = build_frame(1, 0, &[b"data"]);
        let len = frame.len();
        frame[len - 8..].copy_from_slice(&(len as i64).to_le_bytes());
        let err = parse_frame(&frame).unwrap_err();
        assert!(matches!(err, FrameError::Malformed(_)));
    }

    #[test]
    fn rejects_overlapping_column_ranges() {
        let mut frame = build_frame(1, 0, &[b"first", b"second"]);
        let toc_pos = usize::try_from(i64::from_le_bytes(
            frame[frame.len() - 8..].try_into().unwrap(),
        ))
        .unwrap();
        let first_start = frame[toc_pos..toc_pos + 8].to_vec();
        frame[toc_pos + TOC_ENTRY_SIZE..toc_pos + TOC_ENTRY_SIZE + 8].copy_from_slice(&first_start);

        assert_eq!(
            parse_frame(&frame),
            Err(FrameError::Malformed("column ranges overlap"))
        );
    }

    #[test]
    fn rejects_hostile_column_count_without_allocating() {
        let mut frame = format!("&6 1 {} 0 0\n", usize::MAX).into_bytes();
        let toc_pos = frame.len() as i64;
        frame.extend_from_slice(&toc_pos.to_le_bytes());
        assert_eq!(
            parse_frame(&frame),
            Err(FrameError::Malformed("table of contents length overflows"))
        );
    }

    proptest! {
        #[test]
        fn arbitrary_frame_bytes_return_bounded_results(
            frame in prop::collection::vec(any::<u8>(), 0..4096),
        ) {
            if let Ok(parsed) = parse_frame(&frame) {
                prop_assert!(parsed.columns.iter().all(|column| column.len() <= frame.len()));
            }
            let _ = parse_frame_header(&frame);
        }
    }
}
