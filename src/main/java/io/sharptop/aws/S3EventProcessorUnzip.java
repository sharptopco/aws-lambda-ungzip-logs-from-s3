package io.sharptop.aws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.FilenameUtils;

import java.io.*;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;


public class S3EventProcessorUnzip implements RequestHandler<S3Event, String> {

    @Override
    public String handleRequest(S3Event s3Event, Context context) {

        try {
            for (S3EventNotificationRecord record : s3Event.getRecords()) {
                String srcBucket = record.getS3().getBucket().getName();
                String srcKey = getSourceKey(record);

                if (!isGZipFile(srcKey)) return "";

                // Download the gzip from S3 into a stream
                AmazonS3 s3Client = new AmazonS3Client();
                byte[] bytes = getUnzippedBytes(srcBucket, srcKey, s3Client);
                writeUnzippedFile(srcBucket, srcKey, s3Client, bytes);

                //delete zip file when done
                System.out.println("Deleting gzip file " + srcBucket + "/" + srcKey + "...");
                s3Client.deleteObject(new DeleteObjectRequest(srcBucket, srcKey));

                System.out.println("Done.");
            }
            return "Ok";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getSourceKey(S3EventNotificationRecord record) throws UnsupportedEncodingException {

        // Object key may have spaces or unicode non-ASCII characters.
        String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
        srcKey = URLDecoder.decode(srcKey, "UTF-8");
        return srcKey;
    }

    private boolean isGZipFile(String srcKey) {

        // Detect file type
        Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey);
        if (!matcher.matches()) {
            System.out.println("Unable to detect file type for key " + srcKey);
            return false;
        }
        String extension = matcher.group(1).toLowerCase();
        if (!"gz".equals(extension)) {
            System.out.println("Skipping non-zip file " + srcKey + " with extension " + extension);
            return false;
        }
        return true;
    }

    private byte[] getUnzippedBytes(String srcBucket, String srcKey, AmazonS3 s3Client) throws IOException {

        System.out.println("Extracting zip file " + srcBucket + "/" + srcKey);

        InputStream gzIn = createGzipInputStream(srcBucket, srcKey, s3Client);
        return readInputStreamToBytes(gzIn);
    }

    private void writeUnzippedFile(String srcBucket, String srcKey, AmazonS3 s3Client, byte[] bytes) throws IOException {

        System.out.println("Writing " + bytes.length + " extracted bytes.");

        InputStream s3In = new ByteArrayInputStream(bytes);
        ObjectMetadata metadata = createObjectMetadata(bytes.length, "text/plain");
        String fileName = srcKey.replaceAll("\\.gz", ".log");
        s3Client.putObject(srcBucket, FilenameUtils.getFullPath(srcKey) + fileName, s3In, metadata);
        s3In.close();
    }

    private ObjectMetadata createObjectMetadata(int size, String mimeType) {

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(size);
        meta.setContentType(mimeType);
        return meta;
    }

    private InputStream createGzipInputStream(String srcBucket, String srcKey, AmazonS3 s3Client) throws IOException {

        S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
        return new GZIPInputStream(s3Object.getObjectContent());
    }

    private byte[] readInputStreamToBytes(InputStream in) throws IOException {

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int len;
        byte[] buffer = new byte[ 1024 ];
        while ((len = in.read(buffer)) > 0) {
            out.write(buffer, 0, len);
        }
        byte[] bytes = out.toByteArray();

        out.close();
        in.close();

        return bytes;
    }

}
