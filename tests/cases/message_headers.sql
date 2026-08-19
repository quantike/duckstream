-- `headers => true` emits a VARCHAR(aliased JSON) column with serialized headers.
-- Messages without headers yield NULL. Header values are always arrays
-- (multi-valued by NATS spec), so `->` extracts the array and `->> '$[0]'`
-- gets the first value as text. NATS system headers (Nats-*) are filtered out.
SELECT
    seq,
    headers->'$.Content-Type'->>'$[0]' AS content_type,
    headers->'$.X-Trace-Id'->>'$[0]'   AS trace_id
FROM read_jetstream('${STREAM}', url => '${NATS_URL}', headers => true)
ORDER BY seq;
