package com.yourcompany.connect.s3.format;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
@Builder
public class FileMetadata {
    @JsonProperty("topic")
    private String topic;
    
    @JsonProperty("partition")
    private Integer partition;
    
    @JsonProperty("start_offset")
    private Long startOffset;
    
    @JsonProperty("end_offset")
    private Long endOffset;
    
    @JsonProperty("record_count")
    private Long recordCount;
    
    @JsonProperty("file_name")
    private String fileName;
    
    @JsonProperty("s3_path")
    private String s3Path;
    
    @JsonProperty("format")
    private String format;
    
    @JsonProperty("created_time")
    private Long createdTime;
    
    @JsonProperty("timestamp")
    private Long timestamp;
    
    // Optional additional fields
    @JsonProperty("custom_fields")
    private Map<String, Object> customFields;
    
    @JsonProperty("connector_name")
    private String connectorName;
    
    @JsonProperty("task_id")
    private Integer taskId;
}