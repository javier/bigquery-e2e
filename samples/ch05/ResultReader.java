import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 * Translates f / v results from BigQuery table listing or query results
 * into {field1 : value1, field2 : value2, ...} JSON output.
 */
public class ResultReader {
  private final PrintStream printer;
  public ResultReader() {
    this(System.out);
  }
  public ResultReader(PrintStream stream) {
    this.printer = stream;
  }

  private <T extends Map<String, Object>> T fixup(T parent, Object obj) {
    // Because of the cell recursion, we have to do something slightly
    // goofy:  We turn the cell/row into a map, then turn it back into a 
    // cell/row. This works around some Java type inconsistencies between
    // neted calls and outer ones. 
    @SuppressWarnings("unchecked")
    Map<String, Object> valueMap = (Map<String, Object>) obj;
    parent.putAll(valueMap);
    return parent;
  }

  private void printCellValue(TableFieldSchema field, TableCell cell) {
    if (Data.isNull(cell.getV())) {
      printer.append("null");
    } else if (field.getType().toLowerCase().equals("record")) {
      TableRow tableRow = fixup(new TableRow(), cell.getV());
      printCells(field.getFields(), tableRow.getF());
    } else {
      // Everything that isn't a record can be printed as a string.
      printer.format("\"%s\"", cell.getV().toString());
    }
  }

  private void printCell(TableFieldSchema field, TableCell cell) {
    printer.format("\"%s\": ", field.getName());
    String mode = field.getMode();
    if (mode != null && mode.toLowerCase().equals("repeated")) {
      // We've got a repeated field here. This is actually a list of
      // values.
      printer.append("[");
      if (!Data.isNull(cell)) {

        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) cell.getV();

        for (int ii = 0; ii < values.size(); ii += 1) {
          if (ii != 0) {
            printer.append(", ");
          }
          TableCell innerCell = fixup(new TableCell(), values.get(ii));
          printCellValue(field, innerCell);
        }
      }
      printer.append("]");
    } else {
      printCellValue(field, cell);
    }
  }

  private void printCells(List<TableFieldSchema> fields, 
      List<TableCell> cells) {
    printer.append("{");
    for (int ii = 0; ii < fields.size(); ii += 1) {
      if (ii != 0) {
        printer.append(", ");
      }
      TableFieldSchema field = fields.get(ii);
      TableCell cell = fixup(new TableCell(), cells.get(ii));
      printCell(field, cell);
    }
    printer.append("}");
  }

  public void printRows(TableSchema schema, List<TableRow> rows) {
    for (TableRow row : rows) {
      printCells(schema.getFields(), row.getF());
    }
  }

  public void print(QueryResponse response) {
    printRows(response.getSchema(), response.getRows());
  }

  public void print(GetQueryResultsResponse response) {
    printRows(response.getSchema(), response.getRows());
  }

  public void print(TableSchema schema, TableDataList response) {
    printRows(schema, response.getRows());
  }
}
