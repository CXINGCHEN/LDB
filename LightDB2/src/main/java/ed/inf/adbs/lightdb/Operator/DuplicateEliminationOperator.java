package ed.inf.adbs.lightdb.Operator;

import ed.inf.adbs.lightdb.Utils.Tuple;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * duplicate elimination and DISTINCT
 */
public class DuplicateEliminationOperator extends Operator {

    private PlainSelect plainSelect; // sql
    private Operator operator; // child
    private Tuple preTuple;
    private BufferedWriter writer;

    /**
     * Constructor
     *
     * @param plainSelect sql
     * @param operator child
     * @param outputFile output path
     */
    public DuplicateEliminationOperator(PlainSelect plainSelect, Operator operator, String outputFile) {
        this.plainSelect = plainSelect;
        this.operator = operator;
        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return next tuple
     */
    @Override
    public Tuple getNextTuple() {
        // if sql doesn't contain distinct, call child's next tuple
        if (plainSelect.getDistinct() == null) {
            return operator.getNextTuple();
        }

        // first tuple, doesn't duplicate, just get child's next tuple
        if (preTuple == null) {
            preTuple = operator.getNextTuple();
            return preTuple;
        }

        Tuple currTuple;
        // check if 2 neighbor tuple is the same
        while ((currTuple = operator.getNextTuple()) != null) {
            if (!preTuple.equals(currTuple)) {
                preTuple = currTuple;
                return currTuple;
            }
        }
        return null;
    }

    /**
     * Call its child's reset method
     */
    @Override
    public void reset() {
        this.operator.reset();
        this.preTuple = null;
    }

    /**
     * Convert tuple that an int array into string and store it in file
     *
     * @param tuple data to output
     */
    @Override
    public void dump(Tuple tuple) throws IOException {
        try {
            writer.write(String.valueOf(tuple));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
