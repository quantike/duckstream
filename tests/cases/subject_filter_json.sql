-- Subject filter with NATS wildcard semantics, extracting JSON fields as columns.
-- `orders.us.*` matches one trailing token, so eu.* rows are excluded.
SELECT seq, "id", total
FROM read_jetstream(
    '${STREAM}',
    url          => '${NATS_URL}',
    subject      => '${SUBJECT_PREFIX}.orders.us.*',
    json_extract => ['id', 'total']
)
ORDER BY seq;
