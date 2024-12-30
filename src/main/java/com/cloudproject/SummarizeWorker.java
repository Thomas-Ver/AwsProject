package com.cloudproject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.Locale;

public class SummarizeWorker implements RequestHandler<S3Event, String> {
  private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
  private static final String OUTPUT_BUCKET = "your-output-bucket-name";
  
  private static final DateTimeFormatter INPUT_FORMATTER = new DateTimeFormatterBuilder()
    .appendPattern("dd/MM/yyyy hh:mm:ss a")
    .toFormatter(Locale.ENGLISH);
  
  public String processCsv(InputStream inputStream) throws IOException, CsvException {
    Map<String, AggregatedData> dailyTraffic = new HashMap<>();
    
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
         CSVReader csvReader = new CSVReader(reader)) {
      
      List<String[]> records = csvReader.readAll();
      
      // Process records (skip header)
      for (int i = 1; i < records.size(); i++) {
        String[] record = records.get(i);
        if (record.length >= 9) { // Make sure we have enough fields
          processRecord(record, dailyTraffic);
        }
      }
      
      return convertToOutput(dailyTraffic);
    }
  }
  
  private void processRecord(String[] record, Map<String, AggregatedData> dailyTraffic) {
    try {
      // Extract date from timestamp (field 6)
      String timestampStr = record[6].trim();
      if (timestampStr.isEmpty()) {
        return;
      }
      
      // Parse the date using the correct format
      LocalDateTime timestamp = LocalDateTime.parse(timestampStr, INPUT_FORMATTER);
      String date = timestamp.toLocalDate().toString();
      
      // Get source IP (field 1) and destination IP (field 3)
      String sourceIp = record[1].trim();
      String destIp = record[3].trim();
      
      if (sourceIp.isEmpty() || destIp.isEmpty()) {
        return;
      }
      
      // Create key (date,sourceIP,destIP)
      String key = String.format("%s,%s,%s", date, sourceIp, destIp);
      
      // Parse numeric values with error handling
      long flowDuration = parseLongSafely(record[7]);
      long forwardPackets = parseLongSafely(record[8]);
      
      // Update aggregated data
      dailyTraffic.computeIfAbsent(key, k -> new AggregatedData())
        .addData(flowDuration, forwardPackets);
      
    } catch (Exception e) {
      System.err.println("Error processing record: " + Arrays.toString(record));
      System.err.println("Error details: " + e.getMessage());
    }
  }
  
  private long parseLongSafely(String value) {
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException | NullPointerException e) {
      return 0L;
    }
  }
  
  private String convertToOutput(Map<String, AggregatedData> dailyTraffic) {
    StringBuilder output = new StringBuilder();
    output.append("date,source_ip,destination_ip,total_flow_duration,total_forward_packets\n");
    
    dailyTraffic.entrySet().stream()
      .sorted(Map.Entry.comparingByKey())
      .forEach(entry -> {
        output.append(entry.getKey())
          .append(",")
          .append(entry.getValue().totalDuration)
          .append(",")
          .append(entry.getValue().totalPackets)
          .append("\n");
      });
    
    return output.toString();
  }
  
  private static class AggregatedData {
    long totalDuration = 0;
    long totalPackets = 0;
    
    void addData(long duration, long packets) {
      totalDuration += duration;
      totalPackets += packets;
    }
  }
  
  @Override
  public String handleRequest(S3Event s3Event, Context context) {
    try {
      String sourceBucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
      String sourceKey = s3Event.getRecords().get(0).getS3().getObject().getKey();
      String outputKey = "daily_summary_" + java.time.LocalDate.now() + "_" + sourceKey;
      
      S3Object s3Object = s3Client.getObject(new GetObjectRequest(sourceBucket, sourceKey));
      String processedCsv = processCsv(s3Object.getObjectContent());
      
      uploadToS3(processedCsv, outputKey);
      
      return "Successfully processed " + sourceKey;
      
    } catch (Exception e) {
      throw new RuntimeException("Error processing S3 event: " + e.getMessage(), e);
    }
  }
  
  private void uploadToS3(String content, String key) throws IOException {
    byte[] contentBytes = content.getBytes();
    try (InputStream inputStream = new ByteArrayInputStream(contentBytes)) {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(contentBytes.length);
      metadata.setContentType("text/csv");
      
      PutObjectRequest putRequest = new PutObjectRequest(OUTPUT_BUCKET, key, inputStream, metadata);
      s3Client.putObject(putRequest);
    }
  }
}