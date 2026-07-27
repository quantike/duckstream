-- Scan a sequence range via JetStream Direct Get.
SELECT seq, subject, payload::VARCHAR AS payload
FROM read_jetstream(
    '${STREAM}',
    url       => '${NATS_URL}',
    start_seq => 1,
    end_seq   => 100
)
ORDER BY seq;
