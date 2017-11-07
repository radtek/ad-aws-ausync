package com.upsmart.ausync.awss3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yuhang on 17-9-30.
 */
public class AwsS3Wrapper {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(AwsS3Wrapper.class);

    private AmazonS3 s3;

    public AwsS3Wrapper(){
        AWSCredentialsProvider credentials = new ProfileCredentialsProvider("default");
        AmazonS3ClientBuilder awsS3Builder = AmazonS3Client.builder();
        awsS3Builder.setRegion(ConfigurationHelper.SLAVE_AWS_REGION);
        awsS3Builder.setCredentials(credentials);
        s3 = awsS3Builder.build();
    }

    /**
     * bucket下所有文件
     * @return
     */
    public List<AwsS3FileInfo> getAllFilesPath() {
        List<AwsS3FileInfo> files = new ArrayList<>();
        ObjectListing objectListing = s3.listObjects(ConfigurationHelper.SLAVE_AWS_BUCKET_NAME);
        do{
            if(null != objectListing) {
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    AwsS3FileInfo awsS3FileInfo = new AwsS3FileInfo();
                    awsS3FileInfo.path = objectSummary.getKey();
                    awsS3FileInfo.size = objectSummary.getSize();
                    awsS3FileInfo.lastModified = objectSummary.getLastModified();
                    files.add(awsS3FileInfo);
                }
            }
            objectListing = s3.listNextBatchOfObjects(objectListing);
        }
        while(null != objectListing
                && null != objectListing.getObjectSummaries()
                && !objectListing.getObjectSummaries().isEmpty());
        return files;
    }

    /**
     * prefix文件夹下的所有
     * @param prefix
     * @return
     */
    public List<AwsS3FileInfo> getAllFilesPath(String prefix) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(ConfigurationHelper.SLAVE_AWS_BUCKET_NAME);
        listObjectsRequest.setPrefix(prefix);
        List<AwsS3FileInfo> files = new ArrayList<>();
        ObjectListing objectListing = s3.listObjects(listObjectsRequest);
        do {
            if(null != objectListing) {
                for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    AwsS3FileInfo awsS3FileInfo = new AwsS3FileInfo();
                    awsS3FileInfo.path = objectSummary.getKey();
                    awsS3FileInfo.size = objectSummary.getSize();
                    awsS3FileInfo.lastModified = objectSummary.getLastModified();
                    files.add(awsS3FileInfo);
                }
            }
            objectListing = s3.listNextBatchOfObjects(objectListing);
        }
        while(null != objectListing
                && null != objectListing.getObjectSummaries()
                && !objectListing.getObjectSummaries().isEmpty());
        return files;
    }

    public ObjectMetadata getFileInfo(String filePath) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(ConfigurationHelper.SLAVE_AWS_BUCKET_NAME, filePath);
        S3Object s3Object = s3.getObject(getObjectRequest);
        return s3Object.getObjectMetadata();
    }

    /**
     * 下载
     * @param filePath
     * @param file
     */
    public void downloadFile(String filePath, File file) {
        TransferManagerBuilder transferManagerBuilder = TransferManagerBuilder.standard();
        transferManagerBuilder.setS3Client(s3);

        TransferManager tx = transferManagerBuilder.build();
        Download download = tx.download(ConfigurationHelper.SLAVE_AWS_BUCKET_NAME, filePath, file);
        while(true){
            try {
                Thread.sleep(1000);
                if(download.isDone()){
                    LOGGER.info(String.format("download >>> %s - done", filePath));
                    break;
                }
                else{
                    TransferProgress transferProgress = download.getProgress();
                    LOGGER.info(String.format("download >>> %s - %f", filePath, transferProgress.getPercentTransferred()));
                }
            } catch (InterruptedException e) {
                break;
            }
        }
        if(null != tx){
            tx.shutdownNow(false);
        }
    }

    /**
     * 上传
     * @param filePath
     * @param file
     */
    public void uploadFile(String filePath, File file){
        TransferManagerBuilder transferManagerBuilder = TransferManagerBuilder.standard();
        transferManagerBuilder.setS3Client(s3);

        TransferManager tx = transferManagerBuilder.build();
        Upload upload = tx.upload(ConfigurationHelper.SLAVE_AWS_BUCKET_NAME, filePath, file);
        while(true){
            try {
                Thread.sleep(1000);
                if(upload.isDone()){
                    LOGGER.info(String.format("upload >>> %s - done", filePath));
                    break;
                }
                else{
                    TransferProgress transferProgress = upload.getProgress();
                    LOGGER.info(String.format("upload >>> %s - %f", filePath, transferProgress.getPercentTransferred()));
                }
            } catch (InterruptedException e) {
                break;
            }
        }
        if(null != tx){
            tx.shutdownNow(false);
        }
    }

}
