package com.example.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.s3.format.RecordWriter;
import io.confluent.connect.s3.storage.Storage;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

public class CustomAvroRecordWriter implements RecordWriter {
    private final String fileName;
    private final List<SinkRecord> buffer;
    private final MinioMetadataUploader uploader;

    public CustomAvroRecordWriter(String fileName, int expectedRecords) {
        this.fileName = fileName;
        this.buffer = new ArrayList<>(expectedRecords);
        this.uploader = new MinioMetadataUploader();
    }

    @Override
    public void write(SinkRecord record) {
        buffer.add(record);
    }

    @Override
    public void commit() {
        try {
            if (buffer.isEmpty()) return;

            int size = buffer.size();
            long startOffset = buffer.get(0).kafkaOffset();
            long endOffset = buffer.get(size - 1).kafkaOffset();
            String topic = buffer.get(0).topic();
            int partition = buffer.get(0).kafkaPartition();

            Metadata metadata = new Metadata(
                    fileName,
                    size,
                    startOffset,
                    endOffset,
                    topic,
                    partition,
                    "COMPLETED"
            );

            uploader.uploadMetadata(metadata);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {}

    public static class Metadata {
        public String fileName;
        public int recordCount;
        public long startOffset;
        public long endOffset;
        public String topic;
        public int partition;
        public String status;

        public Metadata(String fileName, int recordCount, long startOffset, long endOffset, String topic, int partition, String status) {
            this.fileName = fileName;
            this.recordCount = recordCount;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.topic = topic;
            this.partition = partition;
            this.status = status;
        }
    }
}