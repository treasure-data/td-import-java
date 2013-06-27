package com.treasure_data.bulk_import.integration;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;

@Ignore
public class FileGeneratorMain {
    public static void main(String[] args) throws Exception {
        List<FileGenerator> gens = new ArrayList<FileGenerator>();
        String dirName = "./src/test/resources/in/";

        gens.add(new CSVFileGenerator(dirName + "csvfile-with-time.csv",
                new String[] { "string-value", "int-value", "double-value", "timestamp", "time" }));
        gens.add(new CSVFileGenerator(dirName + "csvfile-with-aliastime.csv",
                new String[] { "string-value", "int-value", "double-value", "timestamp" }));
        gens.add(new CSVFileGenerator(dirName + "csvfile-with-timeformat.csv",
                new String[] { "string-value", "int-value", "double-value", "timeformat" }));
        gens.add(new TrainingDataFileGenerator(dirName + "trainingfile-with-time.msgpack.gz",
                new String[] { "string-value", "int-value", "double-value", "timestamp", "time" }));

        long numRows = 5000;
        long baseTime = 1372305600;

        TrainingDataSet dataset = new TrainingDataSet(numRows, baseTime);
        dataset.createDataFiles(gens.toArray(new FileGenerator[0]));
    }
}
