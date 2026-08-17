//! Async JetStream stream and consumer I/O for the bounded `read_jetstream` modes.
//!
//! This module owns the network-touching operations the scan and consumer
//! paths need: creating a pull consumer ([`create_consumer`]) and resolving a
//! wall-clock timestamp to a stream sequence ([`resolve_time_to_seq`]) via
//! binary search, since JetStream offers no direct timestamp lookup. All
//! functions are `async` and are driven from the synchronous `VTab` callbacks
//! by the runtime owned in the init data.

use async_nats::jetstream;

use crate::error::ScanError;

/// Connect and create a pull consumer for a bounded drain, optionally filtered
/// by subject server-side. Returns the consumer and its `num_pending` count at
/// creation, which bounds the drain and drives the query progress bar.
///
/// When `durable_name` is `Some`, the consumer is persisted server-side and
/// resumes from its stored cursor on subsequent runs; `create_consumer` is
/// idempotent, so an existing durable is attached rather than recreated (its
/// `deliver_policy` is honored only at first creation). When `None`, an
/// ephemeral consumer is created that is reaped shortly after the drain ends.
///
/// Ack policy is derived from the consumer kind: `Explicit` for durables (the
/// cursor advances only as messages are acked on emit, so a cancelled query
/// redelivers unacked messages on the next run) and `None` for ephemerals (no
/// persistence, so acks are irrelevant).
pub async fn create_consumer(
    url: &str,
    stream_name: &str,
    subject: Option<&str>,
    durable_name: Option<&str>,
    deliver_policy: jetstream::consumer::DeliverPolicy,
) -> Result<(jetstream::consumer::PullConsumer, u64), ScanError> {
    use async_nats::jetstream::consumer::{pull, AckPolicy};

    let client = async_nats::connect(url)
        .await
        .map_err(|source| ScanError::Connect {
            url: url.to_string(),
            source,
        })?;
    let context = jetstream::new(client);

    let stream = context
        .get_stream(stream_name)
        .await
        .map_err(|e| ScanError::StreamInfo {
            stream: stream_name.to_string(),
            source: Box::new(e),
        })?;

    let is_durable = durable_name.is_some();
    let ack_policy = if is_durable {
        AckPolicy::Explicit
    } else {
        AckPolicy::None
    };

    let config = pull::Config {
        durable_name: durable_name.map(|s| s.to_string()),
        deliver_policy,
        ack_policy,
        filter_subject: subject.unwrap_or("").to_string(),
        // Ephemeral consumers are reaped shortly after the drain ends. Durable
        // consumers persist (that is the point), so no inactivity reaping.
        inactive_threshold: if is_durable {
            std::time::Duration::ZERO
        } else {
            std::time::Duration::from_secs(30)
        },
        ..Default::default()
    };

    let consumer = stream
        .create_consumer(config)
        .await
        .map_err(|e| ScanError::Consumer {
            stream: stream_name.to_string(),
            source: Box::new(e),
        })?;

    let pending = consumer.cached_info().num_pending;
    Ok((consumer, pending))
}

/// Fetch a single message's server timestamp, in microseconds since the Unix
/// epoch, trying Direct Get first and falling back to the leader-only raw API.
async fn message_time_micros(stream: &jetstream::stream::Stream, seq: u64) -> Option<i64> {
    let msg = match stream.direct_get(seq).await {
        Ok(msg) => msg,
        Err(_) => stream.get_raw_message(seq).await.ok()?,
    };
    Some((msg.time.unix_timestamp_nanos() / 1_000) as i64)
}

/// Resolve a timestamp to a stream sequence by binary search over the sequence
/// space, since JetStream offers no direct timestamp lookup.
///
/// With `lower_bound = true`, returns the first sequence whose message time is
/// `>= target_micros` (for `start_time`). With `lower_bound = false`, returns
/// the last sequence whose message time is `<= target_micros` (for `end_time`).
/// Returns `None` when no sequence satisfies the bound.
///
/// Timestamps in a stream are monotonically non-decreasing with sequence, which
/// makes the search well-defined. Deleted sequences in the middle are handled
/// by probing outward to the nearest existing message.
///
/// PERF: cache probed timestamps. Each probe is a network round-trip
/// (`get_raw_message`/`direct_get`), and the search re-probes overlapping
/// sequences across iterations; memoizing seq -> timestamp would cut the
/// round-trips, and resolving both `start_time` and `end_time` could share one
/// cache.
pub async fn resolve_time_to_seq(
    stream: &jetstream::stream::Stream,
    target_micros: i64,
    first_seq: u64,
    last_seq: u64,
    lower_bound: bool,
) -> Option<u64> {
    if last_seq < first_seq {
        return None;
    }

    let mut lo = first_seq;
    let mut hi = last_seq;
    let mut result: Option<u64> = None;

    while lo <= hi {
        let mid = lo + (hi - lo) / 2;

        // Probe outward from `mid` to find the nearest existing message, since
        // `mid` itself may be a deleted/purged sequence.
        let probe = probe_time_at_or_after(stream, mid, hi).await;
        let Some((seq, micros)) = probe else {
            // No live message from mid..=hi; shrink the upper half.
            if mid == first_seq {
                break;
            }
            hi = mid - 1;
            continue;
        };

        if lower_bound {
            if micros >= target_micros {
                result = Some(seq);
                if seq == first_seq {
                    break;
                }
                hi = seq - 1;
            } else {
                lo = seq + 1;
            }
        } else if micros <= target_micros {
            result = Some(seq);
            lo = seq + 1;
        } else {
            if seq == first_seq {
                break;
            }
            hi = seq - 1;
        }
    }

    result
}

/// Find the first existing message at or after `from`, up to `to`, returning
/// its sequence and timestamp (microseconds).
async fn probe_time_at_or_after(
    stream: &jetstream::stream::Stream,
    from: u64,
    to: u64,
) -> Option<(u64, i64)> {
    let mut seq = from;
    while seq <= to {
        if let Some(micros) = message_time_micros(stream, seq).await {
            return Some((seq, micros));
        }
        seq += 1;
    }
    None
}
