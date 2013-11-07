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
package com.treasure_data.td_import.reader;

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
import java.util.logging.Logger;

import com.treasure_data.td_import.Configuration;
import com.treasure_data.td_import.model.AliasTimeColumnValue;
import com.treasure_data.td_import.model.ColumnType;
import com.treasure_data.td_import.model.ColumnValue;
import com.treasure_data.td_import.model.TimeColumnValue;
import com.treasure_data.td_import.model.TimeValueTimeColumnValue;
import com.treasure_data.td_import.prepare.MySQLPrepareConfiguration;
import com.treasure_data.td_import.prepare.PreparePartsException;
import com.treasure_data.td_import.prepare.Task;
import com.treasure_data.td_import.writer.FileWriter;
import com.treasure_data.td_import.writer.JSONFileWriter;

public class MySQLTableReader extends FileReader<MySQLPrepareConfiguration> {
    private static final Logger LOG = Logger.getLogger(MySQLTableReader.class.getName());

    private static final String QUERY_SAMPLE = "SELECT * FROM %s LIMIT 1;";
    private static final String QUERY = "SELECT * FROM %s;";

    protected Connection conn;
    protected List<String> row = new ArrayList<String>();
    protected int numColumns;
    protected ResultSet resultSet;

    public MySQLTableReader(MySQLPrepareConfiguration conf, FileWriter writer) {
        super(conf, writer);
    }

    @Override
    public void configure(Task task) throws PreparePartsException {
        super.configure(task);

        try {
            Class.forName(Configuration.BI_PREPARE_PARTS_MYSQL_JDBCDRIVER_CLASS);
        } catch (ClassNotFoundException e) {
            throw new PreparePartsException(e);
        } 

        String url = conf.getJdbcUrl();
        String user = conf.getUser();
        String password = conf.getPassword();
        String table = task.getSource().getRawPath();

        // create and test a connection
        try {
            conn = DriverManager.getConnection(url, user, password);
            String msg = String.format("Connected successfully to %s", url);
            System.out.println(msg);
            LOG.info(msg);
        } catch (SQLException e) {
            throw new PreparePartsException(e);
        }

        // sample
        sample(table);

        Statement stat = null;
        try {
            stat = conn.createStatement();
            // TODO optimize the query string
            resultSet = stat.executeQuery(String.format(QUERY, table));
        } catch (SQLException e) {
            throw new PreparePartsException(e);
        }
    }

    private void sample(String table) throws PreparePartsException {
        Statement sampleStat = null;
        ResultSet sampleResultSet = null;

        int timeColumnIndex = -1;
        int aliasTimeColumnIndex = -1;
        try {
            // TODO FIXME
            // here, we should use conn.getMetaData().getColumns(..).
            sampleStat = conn.createStatement();
            sampleResultSet = sampleStat.executeQuery(String.format(QUERY_SAMPLE, table));
            ResultSetMetaData metaData = sampleResultSet.getMetaData();

            numColumns = metaData.getColumnCount();
            if (columnNames == null || columnNames.length == 0) {
                columnNames = new String[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    columnNames[i] = metaData.getColumnName(i + 1);
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

            List<String> firstRow = new ArrayList<String>();
            if (sampleResultSet.next()) {
                for (int i = 0; i < numColumns; i++) {
                    firstRow.add(sampleResultSet.getString(i + 1));
                }
            }

            // initialize types of all columns
            if (columnTypes == null || columnTypes.length == 0) {
                columnTypes = new ColumnType[numColumns];
                for (int i = 0; i < numColumns; i++) {
                    columnTypes[i] = toColumnType(metaData.getColumnType(i + 1));
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

            initializeConvertedRow();

            // check properties of exclude/only columns
            setSkipColumns();

            JSONFileWriter w = null;
            try {
                w = new JSONFileWriter(conf);
                w.setColumnNames(getColumnNames());
                w.setColumnTypes(getColumnTypes());
                w.setSkipColumns(getSkipColumns());
                w.setTimeColumnValue(getTimeColumnValue());

                this.row.addAll(firstRow);

                // convert each column in row
                convertTypesOfColumns();
                // write each column value
                w.next(convertedRow);
                String ret = w.toJSONString();
                String msg = null;
                if (ret != null) {
                    msg = "sample row: " + ret;
                } else  {
                    msg = "cannot get sample row";
                }
                System.out.println(msg);
                LOG.info(msg);
            } finally {
                if (w != null) {
                    w.close();
                }
            }
        } catch (IOException e) {
            throw new PreparePartsException(e);
        } catch (SQLException e) {
            throw new PreparePartsException(e);
        } finally {
            if (sampleResultSet != null) {
                try {
                    sampleResultSet.close();
                } catch (SQLException e) {
                    throw new PreparePartsException(e);
                }
            }

            if (sampleStat != null) {
                try {
                    sampleStat.close();
                } catch (SQLException e) {
                    throw new PreparePartsException(e);
                }
            }
        }
    }

    private static ColumnType toColumnType(int jdbcType)
            throws PreparePartsException {
        switch (jdbcType) {
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            return ColumnType.STRING;
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
            return ColumnType.INT;
        case Types.BIGINT:
            return ColumnType.LONG;
        case Types.FLOAT:
        case Types.DOUBLE:
            return ColumnType.DOUBLE;
        default:
            throw new PreparePartsException("unsupported jdbc type: " + jdbcType);
        }
    }

    @Override
    public boolean readRow() throws IOException {
        row.clear();
        try {
            boolean hasNext = resultSet.next();
            if (!hasNext) {
                return false;
            }

            for (int i = 0; i < numColumns; i++) {
                row.add(i, resultSet.getString(i + 1));
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }

        return true;
    }

    @Override
    public void convertTypesOfColumns() throws PreparePartsException {
        for (int i = 0; i < row.size(); i++) {
            columnTypes[i].convertType(row.get(i), convertedRow.getValue(i));
        }
    }

    @Override
    public String getCurrentRow() {
        return row.toString();
    }

    @Override
    public void close() throws IOException {
        super.close();

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }
    }
}
