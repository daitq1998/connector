package com.example.s3;

import io.confluent.connect.s3.format.Format;
import io.confluent.connect.s3.format.RecordWriter;
import io.confluent.connect.s3.format.RecordWriterProvider;
import io.confluent.connect.s3.storage.Storage;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Set;

public class CustomAvroFormat implements Format<SinkRecord> {
    @Override
    public RecordWriterProvider<SinkRecord> getRecordWriterProvider() {
        return new RecordWriterProvider<SinkRecord>() {
            @Override
            public RecordWriter getRecordWriter(Storage storage, String fileName) {
                return new CustomAvroRecordWriter(fileName, 1000);
            }

            @Override
            public Set<Class<? extends Throwable>> getRecoverableExceptions() {
                return null;
            }
        };
    }

    @Override
    public String getExtension() {
        return ".avro";
    }
}