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
import java.sql.Statement;

import com.treasure_data.bulk_import.prepare_parts.PrepareConfiguration;
import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;
import com.treasure_data.bulk_import.prepare_parts.PrepareProcessor;
import com.treasure_data.bulk_import.writer.FileWriter;

public class MySQLTableReader extends FileReader {

    protected String sql;

    public MySQLTableReader(PrepareConfiguration conf, FileWriter writer) {
        super(conf, writer);
    }

    @Override
    public void configure(PrepareProcessor.Task task) throws PreparePartsException {
        super.configure(task);

        
    }

    @Override
    public boolean readRow() throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    public static void main(String[] args) throws Exception {
        try {
            Class.forName("com.mysql.jdbc.Driver"); 

            String url = "jdbc:mysql://localhost/mugadb";
            String user="root";
            String pass ="";

            Connection conn = DriverManager.getConnection(url, user, pass);
            Statement stat = conn.createStatement();

            // create table
            //stat.execute("CREATE TABLE mugatbl (name char(32), id int, time long)");
            //System.out.println("create table");

            // insert data from the table
            /*
            long baseTime = new java.util.Date().getTime() / 1000 / 3600 * 3600;
            for (int i = 0; i < 50; i++) {
                String sql = String.format("INSERT INTO mugatbl (name, id, time) VALUES (\"%s\", %d, %d);", "muga" + i, i, baseTime + (i * 20));
                System.out.println(sql);
                stat.execute(sql);
            }
             */

            ResultSet rs = stat.executeQuery("SELECT * FROM mugatbl");
            ResultSetMetaData md = rs.getMetaData();
            int numCols = md.getColumnCount();
            for (int i = 1; i <= numCols; i++) {
                System.out.println("col name: " + md.getColumnName(i));
                System.out.println("col type: " + md.getColumnType(i));
            }
            
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }   
    }
}
