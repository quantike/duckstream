-- Per-subject message counts, one row per distinct subject. Counts are
-- deterministic for this case's own stream; the filter query narrows to the
-- us.> subtree, proving the server-side filter is applied.
SELECT subject, messages
FROM jetstream_subjects('${STREAM}', url => '${NATS_URL}')
ORDER BY subject;

SELECT subject, messages
FROM jetstream_subjects('${STREAM}', subject => '${SUBJECT_PREFIX}.orders.us.>', url => '${NATS_URL}')
ORDER BY subject;
