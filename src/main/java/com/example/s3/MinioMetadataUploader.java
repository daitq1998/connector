package com.example.s3;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MinioMetadataUploader {
    private final MinioClient client;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String bucket = System.getenv("METADATA_BUCKET");

    public MinioMetadataUploader() {
        this.client = MinioClient.builder()
                .endpoint(System.getenv("MINIO_ENDPOINT"))
                .credentials(System.getenv("MINIO_ACCESS_KEY"), System.getenv("MINIO_SECRET_KEY"))
                .build();
    }

    public void uploadMetadata(Object metadata) throws Exception {
        String json = objectMapper.writeValueAsString(metadata);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);

        client.putObject(
                PutObjectArgs.builder()
                        .bucket(bucket)
                        .object("metadata/" + System.currentTimeMillis() + ".json")
                        .stream(new ByteArrayInputStream(bytes), bytes.length, -1)
                        .contentType("application/json")
                        .build()
        );
    }
}