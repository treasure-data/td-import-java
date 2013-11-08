//
// Treasure Data Bulk-Import Tool in Java
//
// Copyright (C) 2012 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.td_import.source;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Source extends Source {
    private static final Logger LOG = Logger.getLogger(S3Source.class.getName());

    public static List<Source> createSources(SourceDesc desc) {
        String rawPath = desc.getPath();
        String[] elm = rawPath.split("/");

        String bucket = elm[0];
        String basePath = rawPath.substring(bucket.length() + 1, rawPath.length());

        AmazonS3Client client = createAmazonS3Client(desc);
        List<String> srcNames = getSourceNames(client, bucket, basePath);
        List<Source> srcs = new ArrayList<Source>();
        for (String srcName : srcNames) {
            srcs.add(new S3Source(client, rawPath, bucket, srcName));
        }

        return srcs;
    }

    static AmazonS3Client createAmazonS3Client(SourceDesc desc) {
        String accessKey = desc.getUser();
        if (accessKey == null || accessKey.isEmpty()) {
            throw new IllegalArgumentException("S3 AccessKey is null or empty.");
        }
        String secretAccessKey = desc.getHost();
        if (secretAccessKey == null || secretAccessKey.isEmpty()) {
            throw new IllegalArgumentException("S3 SecretAccessKey is null or empty.");
        }
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretAccessKey);

        ClientConfiguration conf = new ClientConfiguration();
        conf.setProtocol(Protocol.HTTP); // TODO
        conf.setMaxConnections(10); // SDK default: 50 // TODO
        conf.setMaxErrorRetry(5); // SDK default: 3 // TODO
        conf.setSocketTimeout(8 * 60 * 1000); // SDK default: 50 * 1000 // TODO

        return new AmazonS3Client(credentials, conf);
    }

    static List<String> getSourceNames(AmazonS3Client client, String bucket, String basePath) {
        String prefix;
        int index = basePath.indexOf('*');
        if (index >= 0) {
            prefix = basePath.substring(0, index);
        } else {
            prefix = basePath;
        }

        LOG.info(String.format("list s3 files: bucket=%s, basePath=%s, prefix=%s",
                bucket, basePath, prefix));

        List<String> srcNames = new ArrayList<String>();
        String lastKey = prefix;
        do {
            ObjectListing listing = client.listObjects(new ListObjectsRequest(
                    bucket, prefix, lastKey, null, 1024));
            for(S3ObjectSummary s : listing.getObjectSummaries()) {
                srcNames.add(s.getKey());
            }
            lastKey = listing.getNextMarker();
        } while (lastKey != null);

        return filterSourceNames(srcNames, basePath);
    }

    static List<String> filterSourceNames(List<String> names, String basePath) {
        String regex = basePath.replace("*", "([^\\s]*)");
        Pattern pattern = Pattern.compile(regex);

        LOG.info(String.format("regex matching: regex=%s", regex));

        List<String> matched = new ArrayList<String>();
        for (String name : names) {
            Matcher m = pattern.matcher(name);
            if (m.matches()) {
                matched.add(name);
            }
        }
        return matched;
    }

    protected AmazonS3Client client;

    protected String bucket;
    protected String key;
    protected String rawPath;

    S3Source(AmazonS3Client client, String rawPath, String bucket, String key) {
        super(bucket + "/" + key);
        this.client = client;
        this.rawPath = rawPath;
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        LOG.info(String.format("get s3 file: bucket=%s, key=%s", bucket, key));
        S3Object object = client.getObject(new GetObjectRequest(bucket, key));

        if (object != null) {
            return object.getObjectContent();
        } else {
            throw new IOException("s3 file is null.");
        }
    }

    @Override
    public String toString() {
        return String.format("s3-src(bucket=%s,path=%s)", bucket, path);
    }
}
