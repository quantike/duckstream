-- Extract protobuf fields from a pre-compiled FileDescriptorSet
-- (the `buf build -o` / `protoc --descriptor_set_out` artifact form).
-- The harness writes ${DESCRIPTORS} from proto_descriptors.proto before running.
SELECT seq, id, total, status
FROM read_jetstream('${STREAM}',
    url               => '${NATS_URL}',
    proto_descriptors => '${DESCRIPTORS}',
    proto_message     => 'shop.Order',
    proto_extract     => ['id', 'total', 'status']);
