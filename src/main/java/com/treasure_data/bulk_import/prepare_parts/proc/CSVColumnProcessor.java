package com.treasure_data.bulk_import.prepare_parts.proc;

import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.util.CsvContext;

import com.treasure_data.bulk_import.prepare_parts.PreparePartsException;

public class CSVColumnProcessor implements ColumnProcessor, CellProcessor {

    private String columnName;
    private com.treasure_data.bulk_import.prepare_parts.FileWriter writer;

    public CSVColumnProcessor(String columnName,
            com.treasure_data.bulk_import.prepare_parts.FileWriter writer) {
        this.writer = writer;
    }

    @Override
    public Object execute(Object value, CsvContext context) {
        System.out.println("name=" + columnName + ", value=" + value);
        try {
            writer.writeString(columnName);
            if (value == null) {
                writer.writeNil();
                return null;
            }

            writer.writeString((String) value);
            return null;
        } catch (PreparePartsException e) {
            throw new RuntimeException(e); // TODO
        }
    }
}
