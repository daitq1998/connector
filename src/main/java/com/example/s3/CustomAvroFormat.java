package com.yourcompany.connect.s3.format;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.format.avro.AvroFormat;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.Format;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.format.SchemaFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomS3AvroFormat implements Format<S3SinkConnectorConfig, String> {
    
    private static final Logger log = LoggerFactory.getLogger(CustomS3AvroFormat.class);
    
    private final AvroFormat delegateFormat;
    private final MetadataEventService metadataService;
    private final boolean metadataEnabled;
    
    public CustomS3AvroFormat(S3Storage storage) {
        this.delegateFormat = new AvroFormat(storage);
        this.metadataService = new MetadataEventService();
        
        // Enable/disable metadata từ config
        this.metadataEnabled = Boolean.parseBoolean(
            System.getProperty("s3.metadata.enabled", "true")
        );
        
        log.info("CustomS3AvroFormat initialized with metadata enabled: {}", metadataEnabled);
    }
    
    @Override
    public RecordWriterProvider<S3SinkConnectorConfig> getRecordWriterProvider() {
        RecordWriterProvider<S3SinkConnectorConfig> originalProvider = 
            delegateFormat.getRecordWriterProvider();
            
        if (metadataEnabled) {
            return new CustomAvroRecordWriterProvider(originalProvider, metadataService);
        } else {
            log.info("Metadata disabled, using original provider");
            return originalProvider;
        }
    }
    
    @Override
    public SchemaFileReader<S3SinkConnectorConfig, String> getSchemaFileReader() {
        return delegateFormat.getSchemaFileReader();
    }
    
    @Override
    public Object getHiveFactory() {
        return delegateFormat.getHiveFactory();
    }
}