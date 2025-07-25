package com.yourcompany.connect.s3.format;

import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.format.RecordWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class CustomAvroRecordWriter implements RecordWriter {
    
    private static final Logger log = LoggerFactory.getLogger(CustomAvroRecordWriter.class);
    
    private final RecordWriter delegate;
    private final MetadataEventService metadataService;
    private final String filename;
    private final S3SinkConnectorConfig config;
    
    // Metadata tracking
    private String currentTopic;
    private Integer currentPartition;
    private Long startOffset;
    private Long endOffset;
    private final AtomicLong recordCount = new AtomicLong(0);
    private final AtomicBoolean metadataTriggered = new AtomicBoolean(false);
    private final long createdTime;
    
    // Safety measures
    private final AtomicBoolean delegateWriteFailed = new AtomicBoolean(false);
    private final int maxRetries;
    
    public CustomAvroRecordWriter(RecordWriter delegate, 
                                 MetadataEventService metadataService, 
                                 String filename,
                                 S3SinkConnectorConfig config) {
        this.delegate = delegate;
        this.metadataService = metadataService;
        this.filename = filename;
        this.config = config;
        this.createdTime = System.currentTimeMillis();
        this.maxRetries = config.getInt("metadata.max.retries", 3);
        
        log.debug("CustomAvroRecordWriter created for file: {}", filename);
    }
    
    @Override
    public void write(SinkRecord record) {
        try {
            // Track metadata từ first record
            if (recordCount.get() == 0) {
                initializeMetadata(record);
            }
            
            // Update metadata
            updateMetadata(record);
            
            // Delegate actual write
            delegate.write(record);
            
        } catch (Exception e) {
            log.error("Error writing record to file {}: {}", filename, e.getMessage());
            delegateWriteFailed.set(true);
            throw e; // Re-throw để Kafka Connect handle
        }
    }
    
    private void initializeMetadata(SinkRecord record) {
        this.currentTopic = record.topic();
        this.currentPartition = record.kafkaPartition();
        this.startOffset = record.kafkaOffset();
        this.endOffset = record.kafkaOffset();
        
        log.debug("Initialized metadata for file {}: topic={}, partition={}, startOffset={}", 
                  filename, currentTopic, currentPartition, startOffset);
    }
    
    private void updateMetadata(SinkRecord record) {
        this.endOffset = record.kafkaOffset();
        long count = recordCount.incrementAndGet();
        
        if (count % 1000 == 0) { // Log progress every 1000 records
            log.debug("File {} progress: {} records, current offset: {}", 
                      filename, count, endOffset);
        }
    }
    
    @Override
    public void commit() {
        try {
            // Trigger metadata event TRƯỚC KHI commit (push lên S3)
            if (!metadataTriggered.get() && !delegateWriteFailed.get()) {
                triggerMetadataEvent();
            }
            
            // Delegate commit - actual push to S3
            delegate.commit();
            
            log.info("Successfully committed file {} with {} records (offsets {}-{})", 
                     filename, recordCount.get(), startOffset, endOffset);
            
        } catch (Exception e) {
            log.error("Error committing file {}: {}", filename, e.getMessage());
            throw e;
        }
    }
    
    @Override
    public void close() {
        try {
            delegate.close();
            log.debug("Closed writer for file: {}", filename);
        } catch (Exception e) {
            log.error("Error closing writer for file {}: {}", filename, e.getMessage());
            // Don't re-throw on close
        }
    }
    
    private void triggerMetadataEvent() {
        if (metadataTriggered.compareAndSet(false, true)) {
            try {
                FileMetadata metadata = buildFileMetadata();
                metadataService.triggerPreWriteEvent(metadata);
                
                log.info("Triggered metadata event for file {}: topic={}, partition={}, " +
                        "records={}, offsets={}-{}", 
                        filename, currentTopic, currentPartition, 
                        recordCount.get(), startOffset, endOffset);
                        
            } catch (Exception e) {
                log.error("Failed to trigger metadata event for file {}: {}", 
                          filename, e.getMessage());
                // Don't fail the main process vì metadata event
            }
        }
    }
    
    private FileMetadata buildFileMetadata() {
        return FileMetadata.builder()
            .topic(currentTopic)
            .partition(currentPartition)
            .startOffset(startOffset)
            .endOffset(endOffset)
            .recordCount(recordCount.get())
            .fileName(extractFileNameFromPath(filename))
            .s3Path(filename)
            .format("avro")
            .createdTime(createdTime)
            .timestamp(System.currentTimeMillis())
            .build();
    }
    
    private String extractFileNameFromPath(String fullPath) {
        // Extract just filename from full S3 path
        // e.g., topics/my-topic/partition=0/my-topic+0+0000000000.avro -> my-topic+0+0000000000.avro
        if (fullPath != null && fullPath.contains("/")) {
            return fullPath.substring(fullPath.lastIndexOf("/") + 1);
        }
        return fullPath;
    }
}
