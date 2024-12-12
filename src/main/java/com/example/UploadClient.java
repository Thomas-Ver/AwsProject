package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;



public class UploadClient {

    public static String bucketName = "rawdata64987321";
    public static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
    Region region = Region.US_EAST_1;
    S3Client s3 = S3Client.builder().region(region).build();
    System.out.println("Path to the csv file that will be upload : ");
    String PathTocsv = scanner.nextLine().trim();
    System.out.println("name of the csv file that will be upload : ");
    String filename = scanner.nextLine().trim();


    PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucketName).key(filename).build();
    s3.putObject(putOb,RequestBody.fromBytes(getObjectFile(PathTocsv + File.separator + filename)));
}


private static byte[] getObjectFile(String filePath) {

FileInputStream fileInputStream = null;
byte[] bytesArray = null;

try {
  File file = new File(filePath);
  bytesArray = new byte[(int) file.length()];
  fileInputStream = new FileInputStream(file);
  fileInputStream.read(bytesArray);

} catch (IOException e) {
  e.printStackTrace();
} finally {
  if (fileInputStream != null) {
    try {
      fileInputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

return bytesArray;
}
}