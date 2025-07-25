package com.yourcompany.connect.s3.format;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomAvroRecordWriterProvider implements RecordWriterProvider<S3SinkConnectorConfig> {
    
    private static final Logger log = LoggerFactory.getLogger(CustomAvroRecordWriterProvider.class);
    
    private final RecordWriterProvider<S3SinkConnectorConfig> delegate;
    private final MetadataEventService metadataService;
    
    public CustomAvroRecordWriterProvider(
            RecordWriterProvider<S3SinkConnectorConfig> delegate,
            MetadataEventService metadataService) {
        this.delegate = delegate;
        this.metadataService = metadataService;
    }
    
    @Override
    public String getExtension() {
        return delegate.getExtension();
    }
    
    @Override
    public RecordWriter getRecordWriter(S3SinkConnectorConfig config, String filename) {
        try {
            RecordWriter originalWriter = delegate.getRecordWriter(config, filename);
            
            log.debug("Creating CustomAvroRecordWriter for file: {}", filename);
            return new CustomAvroRecordWriter(originalWriter, metadataService, filename, config);
            
        } catch (Exception e) {
            log.error("Failed to create custom record writer for {}, falling back to original", 
                      filename, e);
            // Fallback to original writer nếu có lỗi
            return delegate.getRecordWriter(config, filename);
        }
    }
}