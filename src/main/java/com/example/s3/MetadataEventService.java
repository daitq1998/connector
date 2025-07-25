package com.yourcompany.connect.s3.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MetadataEventService {
    
    private static final Logger log = LoggerFactory.getLogger(MetadataEventService.class);
    
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final CloseableHttpClient httpClient;
    
    // Configuration
    private final String webhookUrl;
    private final boolean webhookEnabled;
    private final int timeoutSeconds;
    
    public MetadataEventService() {
        this.objectMapper = new ObjectMapper();
        this.executorService = Executors.newFixedThreadPool(5, r -> {
            Thread t = new Thread(r, "metadata-event-sender");
            t.setDaemon(true);
            return t;
        });
        this.httpClient = HttpClients.createDefault();
        
        // Load configuration
        this.webhookUrl = System.getProperty("s3.metadata.webhook.url", 
                                           "http://localhost:8080/api/file-metadata");
        this.webhookEnabled = Boolean.parseBoolean(
            System.getProperty("s3.metadata.webhook.enabled", "true"));
        this.timeoutSeconds = Integer.parseInt(
            System.getProperty("s3.metadata.timeout.seconds", "10"));
            
        log.info("MetadataEventService initialized: webhookUrl={}, enabled={}, timeout={}s", 
                 webhookUrl, webhookEnabled, timeoutSeconds);
    }
    
    public void triggerPreWriteEvent(FileMetadata metadata) {
        if (!webhookEnabled) {
            log.debug("Webhook disabled, skipping metadata event for file: {}", 
                      metadata.getFileName());
            return;
        }
        
        // Async execution để không block main process
        CompletableFuture.runAsync(() -> {
            try {
                sendWebhook(metadata);
            } catch (Exception e) {
                log.error("Failed to send metadata webhook for file {}: {}", 
                          metadata.getFileName(), e.getMessage());
            }
        }, executorService).orTimeout(timeoutSeconds, TimeUnit.SECONDS)
          .exceptionally(throwable -> {
              log.error("Metadata webhook timeout for file {}: {}", 
                        metadata.getFileName(), throwable.getMessage());
              return null;
          });
    }
    
    private void sendWebhook(FileMetadata metadata) throws Exception {
        HttpPost request = new HttpPost(webhookUrl);
        request.setHeader("Content-Type", "application/json");
        request.setHeader("User-Agent", "S3-Kafka-Connect-Metadata/1.0");
        
        String jsonPayload = objectMapper.writeValueAsString(metadata);
        request.setEntity(new StringEntity(jsonPayload));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            
            if (statusCode >= 200 && statusCode < 300) {
                log.debug("Successfully sent metadata webhook for file: {}", 
                          metadata.getFileName());
            } else {
                log.warn("Webhook returned non-success status {} for file: {}", 
                         statusCode, metadata.getFileName());
            }
        }
    }
    
    public void shutdown() {
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            httpClient.close();
        } catch (Exception e) {
            log.error("Error shutting down MetadataEventService", e);
        }
    }
}