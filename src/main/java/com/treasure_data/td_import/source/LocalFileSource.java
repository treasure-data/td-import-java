//
//Treasure Data Bulk-Import Tool in Java
//
//Copyright (C) 2012 - 2013 Muga Nishizawa
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package com.treasure_data.td_import.source;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class LocalFileSource extends Source {

    public static List<Source> createSources(SourceDesc desc) {
        List<Source> sources = new ArrayList<Source>();
        sources.add(new LocalFileSource(desc.getPath()));
        return sources;
    }

    public LocalFileSource(String fileName) {
        super(fileName);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return new BufferedInputStream(new FileInputStream(getFileName()));
    }

    public String getFileName() {
        return getPath();
    }

    public File getFile() {
        return new File(getFileName());
    }

    @Override
    public long getSize() {
        return getFile().length();
    }

    @Override
    public String toString() {
        return String.format("local-src(path=%s)", getFileName());
    }
}
