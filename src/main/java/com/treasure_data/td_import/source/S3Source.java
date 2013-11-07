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
import java.util.List;

public class S3Source extends Source {
    public static List<Source> createSources(SourceDesc desc) {
        String rawPath = desc.getPath();
        String[] elm = rawPath.split("/");

        String bucket = elm[0];
        String path = rawPath.substring(bucket.length(), rawPath.length());
        String keyId = desc.getUser();
        String secretKey = desc.getHost();

        // TODO FIXME #MN
        return null;
    }

    protected String keyId;
    protected String secretKey;
    protected String bucket;
    protected String path;

    public S3Source(String rawPath, String keyId, String secretKey, String bucket, String path) {
        super(rawPath);
        this.keyId = keyId;
        this.secretKey = secretKey;
        this.bucket = bucket;
        this.path = path;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        throw new UnsupportedOperationException("this method should be declared in sub-class");
    }

    @Override
    public String toString() {
        return String.format("s3-src(keyId=%s,secretKey=%s,bucket=%s,path=%s)",
                keyId, secretKey, bucket, path);
    }
}
