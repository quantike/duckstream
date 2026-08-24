-- Stream catalog: one row per stream, ordered by name. Only deterministic
-- columns: created/first_ts/last_ts vary per run and bytes vary with payload
-- length. Filters to this case's stream because the enumeration sees every
-- stream on the shared broker.
SELECT stream, messages, first_seq, last_seq, consumer_count, subjects_count,
       deleted_count, retention, storage, discard, num_replicas, sealed,
       allow_direct, description, subjects
FROM jetstream_streams(url => '${NATS_URL}')
WHERE stream = '${STREAM}'
ORDER BY stream;

-- Exact single-stream selector
SELECT stream, messages, last_seq
FROM jetstream_streams(stream => '${STREAM}', url => '${NATS_URL}');
