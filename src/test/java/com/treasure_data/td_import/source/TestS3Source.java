package com.treasure_data.td_import.source;

import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.Assert.assertEquals;

public class TestS3Source {
    @Test
    public void createAmazonS3Client() {
        S3Client client;
        client = S3Source.createS3Client(SourceDesc
                .create("s3://kkk:sss@s3-ap-northeast-1.amazonaws.com/bucket-name/path/to/file.csv"), "ap-northeast-1");
        assertEquals("ap-northeast-1", client.serviceClientConfiguration().region().id());
    }
}
