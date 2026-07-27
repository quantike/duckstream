-- `format => 'json'` retypes the payload column so the json operators apply.
SELECT
    payload->>'$.id'    AS id,
    payload->>'$.total' AS total
FROM read_jetstream('${STREAM}', url => '${NATS_URL}', format => 'json')
ORDER BY id;
