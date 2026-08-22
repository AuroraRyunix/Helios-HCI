//! The metadata client: everything Sidon knows about *where* data lives, held in Hydra
//! and reached only through Daruk.
//!
//! Two rules this module exists to enforce. First, no CQL is issued anywhere else in the
//! daemon -- the map has one gate. Second, and the one that decides whether this design
//! works at all: **guest acknowledgement never waits on Hydra.** Nothing in here is
//! called from the write path. It is called at open, at drain commit, and at ownership
//! change, and those are the only three.
//!
//! HTTP/1.1 is hand-rolled over a plain TcpStream to loopback. That is not minimalism for
//! its own sake: Daruk is a local process, the payloads are small, and a full client
//! stack would be several hundred crates of dependency for `POST /query`.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::{json, Value};

use crate::err::{Error, Result};

pub struct Daruk {
    addr: String,
    timeout: Duration,
}

/// The outcome of a compare-and-swap. `applied == false` is a *successful request* whose
/// condition was refused -- Daruk answers 200 for both, and collapsing them into one
/// boolean is how a lost race gets mistaken for an outage (or worse, the reverse).
pub struct Cas {
    pub applied: bool,
    pub current: Value,
}

impl Cas {
    /// The value a refused CAS reports for `column`, for error messages that name who
    /// actually won rather than saying "conflict".
    pub fn current_str(&self, column: &str) -> String {
        match self.current.get(column) {
            Some(Value::String(s)) => s.clone(),
            Some(v) if !v.is_null() => v.to_string(),
            _ => "<none>".to_string(),
        }
    }

    pub fn current_i64(&self, column: &str) -> Option<i64> {
        self.current.get(column).and_then(|v| v.as_i64())
    }
}

impl Daruk {
    pub fn new(addr: &str, timeout: Duration) -> Self {
        Daruk { addr: addr.to_string(), timeout }
    }

    /// A plain statement. Reads, and the batched block-map writes that a drain commits
    /// at QUORUM before its one lightweight transaction.
    pub fn query(&self, cql: &str) -> Result<Vec<Value>> {
        let body = self.post("/query", cql.as_bytes())?;
        let parsed: Value = serde_json::from_slice(&body)
            .map_err(|e| Error::meta(format!("daruk /query returned non-JSON: {e}")))?;
        if parsed.get("status").and_then(Value::as_str) != Some("success") {
            let msg = parsed.get("error").and_then(Value::as_str).unwrap_or("unknown");
            return Err(Error::meta(format!("daruk refused statement: {msg}")));
        }
        Ok(parsed
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    /// A typed compare-and-swap against one of Daruk's `LWT_OPS` endpoints.
    pub fn cas(&self, path: &str, params: Value) -> Result<Cas> {
        let payload = serde_json::to_vec(&params)
            .map_err(|e| Error::meta(format!("cannot encode {path} parameters: {e}")))?;
        let body = self.post(path, &payload)?;
        let parsed: Value = serde_json::from_slice(&body)
            .map_err(|e| Error::meta(format!("daruk {path} returned non-JSON: {e}")))?;
        if parsed.get("status").and_then(Value::as_str) != Some("success") {
            let msg = parsed.get("error").and_then(Value::as_str).unwrap_or("unknown");
            return Err(Error::meta(format!("daruk {path} failed: {msg}")));
        }
        // Absent `applied` is a protocol change, not a refusal. Defaulting it to false
        // would turn a Daruk upgrade into silent, universal CAS failure -- every claim
        // refused, every drain stalled, and nothing in the logs saying why.
        let applied = parsed
            .get("applied")
            .and_then(Value::as_bool)
            .ok_or_else(|| Error::meta(format!("daruk {path} answered without 'applied'")))?;
        Ok(Cas {
            applied,
            current: parsed.get("current").cloned().unwrap_or(Value::Null),
        })
    }

    fn post(&self, path: &str, body: &[u8]) -> Result<Vec<u8>> {
        let mut stream = TcpStream::connect(&self.addr)
            .map_err(|e| Error::meta(format!("cannot reach daruk at {}: {e}", self.addr)))?;
        stream.set_read_timeout(Some(self.timeout)).ok();
        stream.set_write_timeout(Some(self.timeout)).ok();
        stream.set_nodelay(true).ok();

        let mut req = Vec::with_capacity(body.len() + 160);
        req.extend_from_slice(
            format!(
                "POST {path} HTTP/1.1\r\nHost: daruk\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        );
        req.extend_from_slice(body);
        stream.write_all(&req).map_err(|e| Error::meta(format!("daruk write failed: {e}")))?;

        let mut reader = BufReader::new(stream);
        let mut status = String::new();
        reader
            .read_line(&mut status)
            .map_err(|e| Error::meta(format!("daruk read failed: {e}")))?;
        let code: u16 = status
            .split_whitespace()
            .nth(1)
            .and_then(|c| c.parse().ok())
            .ok_or_else(|| Error::meta(format!("daruk sent a malformed status line: {status:?}")))?;

        let mut content_length: Option<usize> = None;
        loop {
            let mut line = String::new();
            let n = reader
                .read_line(&mut line)
                .map_err(|e| Error::meta(format!("daruk header read failed: {e}")))?;
            if n == 0 || line == "\r\n" || line == "\n" {
                break;
            }
            if let Some((k, v)) = line.split_once(':') {
                if k.eq_ignore_ascii_case("content-length") {
                    content_length = v.trim().parse().ok();
                }
            }
        }

        // Connection: close means EOF is a legitimate terminator when the header is
        // absent, but when it *is* present a short read is truncation, not completion.
        let mut out = Vec::new();
        match content_length {
            Some(len) => {
                out.resize(len, 0);
                reader
                    .read_exact(&mut out)
                    .map_err(|e| Error::meta(format!("daruk body truncated: {e}")))?;
            }
            None => {
                reader
                    .read_to_end(&mut out)
                    .map_err(|e| Error::meta(format!("daruk body read failed: {e}")))?;
            }
        }

        // 400 carries Daruk's own diagnosis in the body; surface it rather than the code.
        if code >= 400 {
            let detail = serde_json::from_slice::<Value>(&out)
                .ok()
                .and_then(|v| v.get("error").and_then(Value::as_str).map(str::to_string))
                .unwrap_or_else(|| String::from_utf8_lossy(&out).trim().to_string());
            return Err(Error::meta(format!("daruk {path} -> HTTP {code}: {detail}")));
        }
        Ok(out)
    }
}

/// Escape a string for single-quoted CQL. Only ever applied to identifiers this daemon
/// generates (uuids, node names, paths) -- there is no user-supplied text in the map --
/// but the map is the one thing that must never be corrupted by a stray quote.
pub fn cql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// The block-map rows for one drain batch, as chunked `BEGIN UNLOGGED BATCH` statements.
///
/// Unlogged is correct here rather than lazy: a logged batch buys atomicity across
/// partitions, and every row in this batch is in the *same* partition (one vdisk), where
/// unlogged batches are already atomic. Paying the batch-log write would be paying for a
/// guarantee we already have.
/// The three values `dfs_vdisks.class` may hold.
///
/// Constants rather than literals because the difference between them is whether a disk
/// can be written, and a typo in a string comparison fails open: `class == "immutabel"`
/// is false, and a golden image every VM is cloned from becomes writable.
///
/// `forming` is the state a snapshot or clone passes through while its map is being
/// copied from its parent. Nothing attaches it, so a copy interrupted halfway leaves a
/// row that says what it is rather than a disk that reads as half zeroes.
pub const CLASS_RW: &str = "rw";
pub const CLASS_IMMUTABLE: &str = "immutable";
pub const CLASS_FORMING: &str = "forming";

/// Rows per `BEGIN UNLOGGED BATCH`. Large enough that a big map is not thousands of round
/// trips, small enough that one statement stays under Scylla's batch warning threshold.
pub const MAP_BATCH: usize = 100;

/// Rows are `(extent_index, egroup_id, egroup_offset, length, vdisk_hash)`.
///
/// `vdisk_hash` is per row and not per call, because one vdisk's map can legitimately
/// mix identities: a clone inherits its parent's extents and stamps its own on anything
/// it rewrites, so the two live side by side in the same map.
///
/// The hash is stored as a signed integer, because CQL has no unsigned type and a
/// bigint that silently wrapped would compare unequal on the way back out and turn every
/// read of a high-hashed vdisk into a misdirected-read error.
pub fn block_map_batches(
    vdisk_id: &str,
    epoch: u64,
    rows: &[(u64, String, u32, u32, u64)],
    per_batch: usize,
) -> Vec<String> {
    let mut out = Vec::new();
    for chunk in rows.chunks(per_batch.max(1)) {
        let mut sql = String::from("BEGIN UNLOGGED BATCH ");
        for (extent_index, egroup_id, egroup_offset, length, vdisk_hash) in chunk {
            sql.push_str(&format!(
                "INSERT INTO hydra.dfs_block_map (vdisk_id, extent_index, egroup_id, \
                 egroup_offset, length, epoch, vdisk_hash) \
                 VALUES ({}, {}, {}, {}, {}, {}, {}); ",
                cql_str(vdisk_id),
                extent_index,
                cql_str(egroup_id),
                egroup_offset,
                length,
                epoch,
                *vdisk_hash as i64
            ));
        }
        sql.push_str("APPLY BATCH;");
        out.push(sql);
    }
    out
}

pub fn json_params(pairs: Vec<(&str, Value)>) -> Value {
    let mut map = serde_json::Map::new();
    for (k, v) in pairs {
        map.insert(k.to_string(), v);
    }
    Value::Object(map)
}

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn _unused_json_marker() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cql_escaping_doubles_quotes() {
        assert_eq!(cql_str("plain"), "'plain'");
        assert_eq!(cql_str("it's"), "'it''s'");
    }

    #[test]
    fn batches_chunk_and_close() {
        let rows = vec![
            (0u64, "eg-a".to_string(), 0u32, 1024u32, 0x1111_2222_3333_4444u64),
            (1, "eg-a".to_string(), 1024, 1024, 0x1111_2222_3333_4444),
            // A different identity in the same map: what a clone looks like once it has
            // rewritten one of the extents it inherited.
            (2, "eg-b".to_string(), 0, 1024, 0xAAAA_BBBB_CCCC_DDDD),
        ];
        let batches = block_map_batches("vd-1", 7, &rows, 2);
        assert_eq!(batches.len(), 2);
        for b in &batches {
            assert!(b.starts_with("BEGIN UNLOGGED BATCH "));
            assert!(b.ends_with("APPLY BATCH;"));
            assert!(b.contains("epoch"));
            assert!(b.contains("vdisk_hash"));
        }
        assert_eq!(batches[0].matches("INSERT INTO").count(), 2);
        assert_eq!(batches[1].matches("INSERT INTO").count(), 1);
        // Written as a signed integer, and the top-bit case is the one that matters:
        // an unsigned rendering would come back as a different number entirely.
        assert!(batches[1].contains(&(0xAAAA_BBBB_CCCC_DDDDu64 as i64).to_string()));
    }

    #[test]
    fn a_high_hash_survives_the_round_trip_through_cql() {
        // The check the footer performs is an equality test, so a hash that does not
        // come back bit-identical turns every read of that vdisk into "misdirected
        // read". Signed is the only integer CQL has; this pins the conversion.
        for h in [0u64, 1, i64::MAX as u64, 1u64 << 63, u64::MAX] {
            let rows = vec![(0u64, "eg".to_string(), 0u32, 4u32, h)];
            let sql = block_map_batches("vd", 0, &rows, 1).remove(0);
            assert!(sql.contains(&(h as i64).to_string()), "{h:#x} missing from {sql}");
            assert_eq!((h as i64) as u64, h);
        }
    }

    #[test]
    fn cas_reports_the_winner_by_name() {
        let cas = Cas {
            applied: false,
            current: serde_json::json!({"owner": "10.10.102.41", "epoch": 4}),
        };
        assert_eq!(cas.current_str("owner"), "10.10.102.41");
        assert_eq!(cas.current_i64("epoch"), Some(4));
        assert_eq!(cas.current_str("missing"), "<none>");
    }
}
