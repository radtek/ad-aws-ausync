package com.upsmart.ausync.awss3;

import com.upsmart.server.configuration.ConfigurationManager;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

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
        System.out.println("finished!");
    }

    @Test
    public void testDownload(){

        String filePath = "precise/170918/2017-09_application_1502259846820_0513_00_full.gz";
        File file = new File("/home/upsmart/works/2017-09_application_1502259846820_0513_00_full.gz");
        awsS3Wrapper.downloadFile(filePath, file);
    }
}
