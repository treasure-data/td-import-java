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
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import com.treasure_data.td_import.Configuration;

/**
 * td import:prepare "s3://access_key_id:secret_access_key@/bucket/key_prefix"
 */
public class S3Source extends Source {
    private static final Logger LOG = Logger.getLogger(S3Source.class.getName());

    public static List<Source> createSources(SourceDesc desc) {
        String rawPath = desc.getPath();
        String[] elm = rawPath.split("/");

        String bucket = elm[0];
        String basePath = rawPath.substring(bucket.length() + 1, rawPath.length());

        System.setProperty("com.amazonaws.sdk.disableCertChecking", "1");

        String region = "";
        try {
            region = getBucketLocation(desc, bucket);
        } catch (S3Exception e) {
            LOG.warning("Failed to get S3 bucket location. Please make sure that your account is given 's3:GetBucketLocation' role.");
            throw e;
        }
        S3Client client = createS3Client(desc, region);
        List<S3Object> s3objects = getSources(client, bucket, basePath);
        List<Source> srcs = new ArrayList<>();
        for (S3Object s3object : s3objects) {
            LOG.info(String.format("create s3-src s3object=%s, rawPath=%s",
                    s3object.key(), rawPath));
            srcs.add(new S3Source(createS3Client(desc, region), rawPath,
                    bucket, s3object.key(), s3object.size()));
        }

        return srcs;
    }

    static S3Client createS3Client(SourceDesc desc, String region) {
        return createClientBuilder(desc).region(Region.of(region)).build();
    }

    static S3ClientBuilder createClientBuilder(SourceDesc desc) {
        String accessKey = desc.getUser();
        if (accessKey == null || accessKey.isEmpty()) {
            throw new IllegalArgumentException("S3 AccessKey is null or empty.");
        }
        String secretAccessKey = desc.getPassword();
        if (secretAccessKey == null || secretAccessKey.isEmpty()) {
            throw new IllegalArgumentException("S3 SecretAccessKey is null or empty.");
        }
        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretAccessKey);

        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
            .maxConnections(Configuration.BI_PREPARE_S3_MAX_CONNECTIONS)
            .socketTimeout(java.time.Duration.ofMillis(Configuration.BI_PREPARE_S3_SOCKET_TIMEOUT));

        ClientOverrideConfiguration overrideConfig = ClientOverrideConfiguration.builder()
            .retryStrategy(b -> b.maxAttempts(Configuration.BI_PREPARE_S3_MAX_ERRORRETRY))
            .build();

        return S3Client.builder()
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .httpClientBuilder(httpClientBuilder)
            .overrideConfiguration(overrideConfig);
    }

    static String getBucketLocation(SourceDesc desc, String bucket) {
        try (S3Client client = createClientBuilder(desc).build()) {
            GetBucketLocationRequest request = GetBucketLocationRequest
                .builder()
                .bucket(bucket)
                .build();
            String bucketRegion = client.getBucketLocation(request).locationConstraintAsString();
            if (bucketRegion == null || bucketRegion.isEmpty()) {
                return "us-east-1";
            } else {
                return bucketRegion;
            }
        }
    }

    static List<S3Object> getSources(S3Client client, String bucket, String basePath) {
        String prefix;
        int index = basePath.indexOf('*');
        if (index >= 0) {
            prefix = basePath.substring(0, index);
        } else {
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(basePath)
                .build();
            HeadObjectResponse headResponse = client.headObject(headRequest);
            S3Object s3object = S3Object.builder()
                .key(basePath)
                .size(headResponse.contentLength())
                .build();

            return Collections.singletonList(s3object);
        }

        LOG.info(String.format("list s3 files by client %s: bucket=%s, basePath=%s, prefix=%s",
                client, bucket, basePath, prefix));

        List<S3Object> s3objects = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(20);
            if (continuationToken != null) {
                requestBuilder.continuationToken(continuationToken);
            }
            ListObjectsV2Response response = client.listObjectsV2(requestBuilder.build());
            for(S3Object s3object : response.contents()) {
                s3objects.add(s3object);
            }
            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);

        return filterSources(s3objects, basePath);
    }

    static List<S3Object> filterSources(List<S3Object> s3objects, String basePath) {
        String regex = basePath.replace("*", "([^\\s]*)");
        Pattern pattern = Pattern.compile(regex);

        LOG.info(String.format("regex matching: regex=%s", regex));

        List<S3Object> matched = new ArrayList<S3Object>();
        for (S3Object s3object : s3objects) {
            Matcher m = pattern.matcher(s3object.key());
            if (m.matches()) {
                matched.add(s3object);
            }
        }
        return matched;
    }

    protected S3Client client;

    protected String bucket;
    protected String key;
    protected long size;
    protected String rawPath;

    S3Source(S3Client client, String rawPath, String bucket, String key, long size) {
        super("s3://"+bucket+"/"+key);
        this.client = client;
        this.bucket = bucket;
        this.key = key;
        this.size = size;
        this.rawPath = rawPath;
    }

    @Override
    public char getSeparatorChar() {
        return '/';
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        LOG.info(String.format("get s3 file by client %s: bucket=%s, key=%s",
                client, bucket, key));
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=0-" + (size - 1))
                .build();
        ResponseInputStream<GetObjectResponse> responseStream = client.getObject(req);

        if (responseStream != null) {
            return responseStream;
        } else {
            throw new IOException("s3 file is null.");
        }
    }

    @Override
    public String toString() {
        return String.format("s3-src(bucket=%s,path=%s)", bucket, path);
    }
}
