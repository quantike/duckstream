-- Durable consumer reading from its server-persisted cursor. The harness runs
-- this twice with messages published in between; the second run sees only those.
SELECT seq, subject, payload::VARCHAR AS payload
FROM read_jetstream(
    '${STREAM}',
    url     => '${NATS_URL}',
    durable => 'it_resume'
)
ORDER BY seq;
