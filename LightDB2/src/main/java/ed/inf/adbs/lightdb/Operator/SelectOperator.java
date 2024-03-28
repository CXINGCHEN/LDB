package ed.inf.adbs.lightdb.Operator;

import ed.inf.adbs.lightdb.Utils.DatabaseCatalog;
import ed.inf.adbs.lightdb.Utils.Tuple;
import ed.inf.adbs.lightdb.parser.SelectExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Realize single-table selection
 * Support queries like SELECT * FROM Boats WHERE Boats.id = 4
 */
public class SelectOperator extends Operator {

    private ScanOperator scanOperator; // use scanOperator.getNextTuple()
    private BufferedWriter writer; // for dump
    private SelectExpressionDeParser selectExpressionDeParser; // extend ExpressionDeParser


    /**
     * Constructor
     *
     * @param expression WHERE clause of a SQL SELECT statement
     * @param tableName  name of the table
     * @param outputFile where the results will be written
     */
    public SelectOperator(Expression expression, String tableName, String outputFile) {
        // ScanOperator as child
        this.scanOperator = new ScanOperator(DatabaseCatalog.getsInstance().getFileByTableName(tableName), outputFile);
        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (expression != null) {
            List<String> schema = DatabaseCatalog.getsInstance().getSchemaByTableName(tableName);
            if (tableName.split(" ").length == 2) {
                DatabaseCatalog.getsInstance().convertToAliasSchema(schema, tableName.split(" ")[1]);
            }
            selectExpressionDeParser = new SelectExpressionDeParser(expression, schema);
        }
    }

    /**
     * Check tuple, if the tuple meets the requirements, return it.
     * @return an instance of Tuple
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple;
        while ((tuple = scanOperator.getNextTuple()) != null) {
            if (selectExpressionDeParser == null) {
                return tuple;
            }
            if (selectExpressionDeParser.checkTuple(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    /**
     * Call its child's reset method
     */
    @Override
    public void reset() {
        scanOperator.reset();
    }

    /**
     * Convert tuple that an int array into string and store it in file
     *
     * @param tuple data to output
     */
    @Override
    public void dump(Tuple tuple) {
        try {
            writer.write(String.valueOf(tuple));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
