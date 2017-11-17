package com.upsmart.ausync.awss3;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.upsmart.ausync.configuration.ConfigurationHelper;
import com.upsmart.ausync.model.BrotherFiles;
import com.upsmart.server.configuration.ConfigurationManager;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yuhang on 17-9-30.
 */
public class AwsS3WrapperTest {

    AwsS3Wrapper awsS3Wrapper;

    @Before
    public void before(){
        PropertyConfigurator.configure("/home/upsmart/works/projects/adserver/ad-ausync/ad-ausync-server/properties/local/log4j.properties");
        ConfigurationManager.initInstance("/home/upsmart/works/projects/adserver/ad-ausync/ad-ausync-server/properties/local/web.config");
        awsS3Wrapper = new AwsS3Wrapper();
    }

    @After
    public void after(){
    }

    @Test
    public void test(){
        List<AwsS3FileInfo> listFiles = awsS3Wrapper.getAllFilesPath();
//        List<AwsS3FileInfo> listFiles = awsS3Wrapper.getAllFilesPath("precise/data/15166");
//        List<AwsS3FileInfo> listFiles = awsS3Wrapper.getAllFilesPath("audience-analytics-webapi");
        for(AwsS3FileInfo file : listFiles){
            System.out.println(String.format("%s, \t(%s),%s",
                    file.path, file.convertSize(), file.lastModified ));
        }
        System.out.println("finished! "+listFiles.size());
    }

    @Test
    public void testDownload(){

        String filePath = "precise/170918/2017-09_application_1502259846820_0513_00_full.gz";
        File file = new File("/home/upsmart/works/2017-09_application_1502259846820_0513_00_full.gz");
        awsS3Wrapper.downloadFile(filePath, file);
    }

    @Test
    public void testUpload(){

        String filePath = "precise/data/1234/2017-11_application_1510565574485_0000_00_full.gz";
        File file = new File("/home/upsmart/works/1234/2017-11_application_1510565574485_0000_00_full.gz");
        awsS3Wrapper.uploadFile(filePath, file);

        filePath = "precise/data/1234/2017-11_application_1510565574485_0000_00_full.gz.md5";
        file = new File("/home/upsmart/works/1234/2017-11_application_1510565574485_0000_00_full.gz.md5");
        awsS3Wrapper.uploadFile(filePath, file);
    }

    @Test
    public void testDownload2(){
        Map<String,BrotherFiles> map = new HashMap<>();

        AwsS3Wrapper awsS3Wrapper = new AwsS3Wrapper();
        String awsFolder = String.format("%s/%s", ConfigurationHelper.SLAVE_AWS_AUDIENCE_PATH, "170920");
        List<AwsS3FileInfo> list = awsS3Wrapper.getAllFilesPath(awsFolder);
        for(AwsS3FileInfo awsS3FileInfo : list){
            String name = awsS3FileInfo.path.substring(awsS3FileInfo.path.lastIndexOf("/")+1);
            String localFile = String.format("%s/%s/%s", ConfigurationHelper.SLAVE_LOCAL_AUDIENCE_PATH, "170920", name);
            File file = new File(localFile);
            awsS3Wrapper.downloadFile(awsS3FileInfo.path, file);

            String prefix = name.substring(0, name.indexOf("."));
            String suffix = name.substring(name.lastIndexOf("."));
            if(!map.containsKey(prefix)){
                map.put(prefix, new BrotherFiles());
            }
            if(suffix.equals(".md5")){
                map.get(prefix).md5FilePath = name;
            }
            else if(suffix.equals(".gz")){
                map.get(prefix).gzipFilePath = name;
            }
            else{
                map.get(prefix).filePath = name;
            }
        }

        for(String k : map.keySet()){
            BrotherFiles b = map.get(k);
            System.out.println(String.format("%s - %s - %s - %s", k, b.filePath, b.gzipFilePath, b.md5FilePath));
        }
    }
}
