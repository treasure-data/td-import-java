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
package com.treasure_data.bulk_import.model;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeColumnSampling extends ColumnSampling {
    private static final SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat yyyyMMdd$1HHmmss = new SimpleDateFormat("yyyyMMdd$1HHmmss");

    private static final String yyyyMMdd_STRF = "%Y%m%d";
    private static final String yyyyMMdd$1HHmmss_STRF = "%Y%m%d$1%H%M%S";
    private static final String[] STRF_LIST = new String[] { yyyyMMdd_STRF, yyyyMMdd$1HHmmss_STRF };

    protected int[] timeScores = new int[] { 0, 0 };

    public TimeColumnSampling(int numRows) {
        super(numRows);
    }

    @Override
    public void parse(String value) {
        super.parse(value);

        {
            ParsePosition pp = new ParsePosition(0);
            Date d = yyyyMMdd.parse(value, pp);
            if (d != null && pp.getErrorIndex() == -1) {
                timeScores[0] += 1;
            }
        }

        {
            ParsePosition pp = new ParsePosition(0);
            Date d = yyyyMMdd$1HHmmss.parse(value, pp);
            if (d != null && pp.getErrorIndex() == -1) {
                timeScores[1] += 1;
            }
        }
    }

    public ColumnType getColumnTypeRank() {
        return super.getRank();
    }

    public String getSTRFTimeFormatRank() {
        int max = -numRows;
        int maxIndex = 0;
        for (int i = 0; i < timeScores.length; i++) {
            if (max <= timeScores[i]) {
                max = timeScores[i];
                maxIndex = i;
            }
        }

        if (maxIndex == 0) {
            return null;
        } else {
            return STRF_LIST[maxIndex];
        }
    }

}
