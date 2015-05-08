package com.treasure_data.td_import.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3Client;

public class TestS3Source {
    @Test
    public void createAmazonS3Client() {
        AmazonS3Client client;
        client = S3Source.createAmazonS3Client(SourceDesc
                .create("s3://kkk:sss@s3-ap-northeast-1.amazonaws.com/bucket-name/path/to/file.csv"));
        assertEquals("ap-northeast-1", client.getRegion().getFirstRegionId());
        client = S3Source.createAmazonS3Client(SourceDesc
                .create("s3://kkk:sss@ds.jp-east.idcfcloud.com/bucket-name/path/to/file.csv"));
        try {
            // There's no AmazonS3Client#getEndpoint();
            // ds.jp-east.idcfcloud.com is not registered as region.
            // Anyway it updates endpoint so it's expected behavior.
            client.getRegion();
            fail();
        } catch (IllegalStateException ise) {
            // expected
        }
    }
}