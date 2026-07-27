-- Ephemeral consumer drains everything currently in the stream, reporting a count.
SELECT count(*) AS n
FROM read_jetstream('${STREAM}', url => '${NATS_URL}', ephemeral => true);
