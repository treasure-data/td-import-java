package com.treasure_data.bulk_import.integration;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;

@Ignore
public class FileGeneratorMain {
    public static void main(String[] args) throws Exception {
        List<FileGenerator> gens = new ArrayList<FileGenerator>();
        String dirName = "./src/test/resources/in/";

        // CSV files
        gens.add(new CSVFileGenerator(dirName + "csvfile-with-time.csv",
                new String[] { "string-value", "int-value", "double-value", "timestamp", "time" }));
        gens.add(new CSVFileGenerator(dirName + "csvfile-with-aliastime.csv",
                new String[] { "string-value", "int-value", "double-value", "timestamp" }));
        gens.add(new CSVFileGenerator(dirName + "csvfile-with-timeformat.csv",
                new String[] { "string-value", "int-value", "double-value", "timeformat" }));

        // header-less CSV files
        gens.add(new HeaderlessCSVFileGenerator(dirName + "headerless-csvfile-with-time.csv",
                new String[] { "string-value", "int-value", "double-value", "timestamp", "time" }));
        gens.add(new HeaderlessCSVFileGenerator(dirName + "headerless-csvfile-with-aliastime.csv",
                new String[] { "string-value", "int-value", "double-value", "timestamp" }));
        gens.add(new HeaderlessCSVFileGenerator(dirName + "headerless-csvfile-with-timeformat.csv",
                new String[] { "string-value", "int-value", "double-value", "timeformat" }));

        // JSON files
        gens.add(new JSONFileGenerator(dirName + "jsonfile-with-time.csv",
                new String[] { "string-value", "int-value", "double-value", "timestamp", "time" }));

        gens.add(new TrainingDataFileGenerator(dirName + "trainingfile-with-time.msgpack.gz",
                new String[] { "string-value", "int-value", "double-value", "timestamp", "time" }));

        long numRows = 5000;
        long baseTime = 1372305600;

        String[]  availableHeader = new String[] {
                "string-value", "int-value", "double-value", "time", "timestamp", "timeformat",
        };
        TrainingDataSet dataset = new TrainingDataSet(numRows, baseTime, availableHeader);
        dataset.createDataFiles(gens.toArray(new FileGenerator[0]));
    }
}
