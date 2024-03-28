package ed.inf.adbs.lightdb.Operator;

import ed.inf.adbs.lightdb.Utils.DatabaseCatalog;
import ed.inf.adbs.lightdb.Utils.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Projection
 * Handle queries like SELECT Sailors.id FROM Sailors WHERE Sailors.age = 20
 */
public class ProjectOperator extends Operator {

    private PlainSelect plainSelect; //sql
    private Operator operator; // Scan or Select Operator
    private List<String> schema;
    private BufferedWriter writer;

    /**
     * Constructor
     *
     * @param plainSelect sql
     * @param operator    child
     * @param outputFile  result file directory
     */
    public ProjectOperator(PlainSelect plainSelect, Operator operator, String outputFile) {
        this.plainSelect = plainSelect;
        this.operator = operator;
        schema = DatabaseCatalog.getsInstance().getSchemas(plainSelect); // get schema
        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get next tuple depends on SelectItems
     *
     * @return Next Tuple
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple = this.operator.getNextTuple(); // use its child operator to get its next tuple

        // if nothing left in the table
        if (tuple == null) {
            return null;
        }

        // if sql is select * , just return the entire row
        if (this.plainSelect.getSelectItems().get(0).toString().equals("*")) {
            return tuple;
        }

        // create an int array depends on how many columns are needed
        int[] intArray = new int[this.plainSelect.getSelectItems().size()];

        // for this tuple, get the value of a column i
        for (int i = 0; i < this.plainSelect.getSelectItems().size(); i++) {
            // get column "i"
            Column column = (Column) ((SelectExpressionItem) this.plainSelect.getSelectItems().get(i)).getExpression();
            // check aliases
            if (DatabaseCatalog.getsInstance().ifUseAlias(this.plainSelect)) {
                intArray[i] = tuple.get(this.schema.indexOf(column.toString()));
            } else {
                intArray[i] = tuple.get(this.schema.indexOf(column.getColumnName()));
            }
        }

        tuple = new Tuple(intArray); // use this int array to create a tuple
        return tuple; // return the tuple
    }

    /**
     * Call its child's reset method
     */
    @Override
    public void reset() {
        operator.reset();
    }

    /**
     * Convert tuple that an int array into string and store it in file
     *
     * @param tuple data to output
     */
    @Override
    public void dump(Tuple tuple)  {
        try {
            writer.write(String.valueOf(tuple));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
