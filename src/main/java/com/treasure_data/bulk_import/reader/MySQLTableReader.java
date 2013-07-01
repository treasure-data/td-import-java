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
package com.treasure_data.bulk_import.reader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.treasure_data.bulk_import.Configuration;
import com.treasure_data.bulk_import.model.AliasTimeColumnValue;
import com.treasure_data.bulk_import.model.ColumnType;
import com.treasure_data.bulk_import.model.ColumnValue;
import com.treasure_data.bulk_import.model.TimeColumnValue;
import com.treasure_data.bulk_import.model.TimeValueTimeColumnValue;
import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.Task;
import com.treasure_data.bulk_import.writer.FileWriter;

public class MySQLTableReader extends FileReader {

    private static final String SAMPLE_QUERY = "SELECT * FROM %s LIMIT 1;";
    private static final String QUERY = "SELECT * FROM %s;";

    protected Connection conn;
    protected List<String> rawRow = new ArrayList<String>();
    protected int numColumns;
    protected ResultSet resultSet;

    public MySQLTableReader(PrepareConfiguration conf, FileWriter writer) {
        super(conf, writer);
    }

    @Override
    public void configure(Task task) throws PreparePartsException {
        super.configure(task);

        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new PreparePartsException(e);
        } 

        String url = conf.getJDBCConnectionURL();
        String user = conf.getJDBCUser();
        String password = conf.getJDBCPassword();
        String table = conf.getJDBCTable();

        // create and test a connection
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            throw new PreparePartsException(e);
        }

        // sample
        sample(table);

        try {
            Statement stat = conn.createStatement();
            // TODO optimize the query string
            resultSet = stat.executeQuery(String.format(QUERY, table));
        } catch (SQLException e) {
            throw new PreparePartsException(e);
        }
    }

    private void sample(String table) throws PreparePartsException {
        Statement stat = null;
        ResultSet rs = null;

        int timeColumnIndex = -1;
        int aliasTimeColumnIndex = -1;
        try {
            stat = conn.createStatement();
            rs = stat.executeQuery(String.format(SAMPLE_QUERY, table));
            ResultSetMetaData metaData = rs.getMetaData();

            numColumns = metaData.getColumnCount();
            if (columnNames == null || columnNames.length == 0) {
                columnNames = new String[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    columnNames[i] = metaData.getColumnName(i);
                }
            }

            // get index of 'time' column
            // [ "time", "name", "price" ] as all columns is given,
            // the index is zero.
            for (int i = 0; i < columnNames.length; i++) {
                if (columnNames[i].equals(
                        Configuration.BI_PREPARE_PARTS_TIMECOLUMN_DEFAULTVALUE)) {
                    timeColumnIndex = i;
                    break;
                }
            }

            // get index of specified alias time column
            // [ "timestamp", "name", "price" ] as all columns and
            // "timestamp" as alias time column are given, the index is zero.
            //
            // if 'time' column exists in row data, the specified alias
            // time column is ignore.
            if (timeColumnIndex < 0 && conf.getAliasTimeColumn() != null) {
                for (int i = 0; i < columnNames.length; i++) {
                    if (columnNames[i].equals(conf.getAliasTimeColumn())) {
                        aliasTimeColumnIndex = i;
                        break;
                    }
                }
            }

            // if 'time' and the alias columns don't exist, ...
            if (timeColumnIndex < 0 && aliasTimeColumnIndex < 0) {
                if (conf.getTimeValue() >= 0) {
                } else {
                    throw new PreparePartsException(
                            "Time column not found. --time-column or --time-value option is required");
                }
            }

            // initialize types of all columns
            if (columnTypes == null | columnTypes.length == 0) {
                columnTypes = new ColumnType[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    columnTypes[i] = toColumnType(metaData.getColumnType(i));
                }
            }

            // initialize time column value
            if (timeColumnIndex >= 0) {
                timeColumnValue = new TimeColumnValue(timeColumnIndex,
                        conf.getTimeFormat());
            } else if (aliasTimeColumnIndex >= 0) {
                timeColumnValue = new AliasTimeColumnValue(
                        aliasTimeColumnIndex, conf.getTimeFormat());
            } else {
                timeColumnValue = new TimeValueTimeColumnValue(
                        conf.getTimeValue());
            }

            initializeConvertedRow(timeColumnValue); // FIXME this argument is not needed?

            // check properties of exclude/only columns
            setSkipColumns();
        } catch (SQLException e) {
            throw new PreparePartsException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    throw new PreparePartsException(e);
                }
            }

            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    throw new PreparePartsException(e);
                }
            }
        }
    }

    private static ColumnType toColumnType(int jdbcType)
            throws PreparePartsException {
        // TODO append more types
        switch (jdbcType) {
            case Types.VARCHAR:
                return ColumnType.STRING;
            case Types.INTEGER:
                return ColumnType.INT;
            case Types.BIGINT:
                return ColumnType.LONG;
        default:
            throw new PreparePartsException(new UnsupportedOperationException(
                    "jdbc type: " + jdbcType));
        }
    }

    @Override
    public boolean readRow() throws IOException {
        try {
            boolean hasNext = resultSet.next();
            if (!hasNext) {
                return false;
            }

            for (int i = 0; i < numColumns; i++) {
                rawRow.add(i, resultSet.getString(i + 1));
            }
            return true;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void convertTypesOfColumns() throws PreparePartsException {
        for (int i = 0; i < rawRow.size(); i++) {
            ColumnValue v = convertedRow.getValue(i);
            columnTypes[i].convertType(rawRow.get(i), v);
            convertedRow.setValue(i, v);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }

    private void setSkipColumns() {
        String[] excludeColumns = conf.getExcludeColumns();
        String[] onlyColumns = conf.getOnlyColumns();
        for (int i = 0; i < columnNames.length; i++) {
            // check exclude columns
            boolean isExcluded = false;
            for (String excludeColumn : excludeColumns) {
                if (columnNames[i].equals(excludeColumn)) {
                    isExcluded = true;
                    break;
                }
            }

            if (isExcluded) {
                skipColumns.add(columnNames[i]);
                continue;
            }

            // check only columns
            if (onlyColumns.length == 0) {
                continue;
            }

            boolean isOnly = false;
            for (String onlyColumn : onlyColumns) {
                if (columnNames[i].equals(onlyColumn)) {
                    isOnly = true;
                    break;
                }
            }

            if (!isOnly) {
                skipColumns.add(columnNames[i]);
                continue; // not needed though,..
            }
        }
    }
}
