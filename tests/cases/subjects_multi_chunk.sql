-- More distinct subjects than the extension's VECTOR_SIZE (2048), so `func`
-- must emit at least two chunks; count(*) proves the boundary is lossless.
SELECT count(*) AS subjects
FROM jetstream_subjects('${STREAM}', url => '${NATS_URL}');
