package ed.inf.adbs.lightdb.Operator;


import ed.inf.adbs.lightdb.Utils.Tuple;
import ed.inf.adbs.lightdb.parser.SelectExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;

import java.io.IOException;
import java.util.List;

/**
 * JoinOperator has 2 child Operators
 * Support sql that contains R.B = S.A
 */
public class JoinOperator extends Operator {

    public static int outerLoopCount = 0;
    public static int innerLoopCount = 0;
    private Operator leftOperator; // select or project or scan or join
    private Operator rightOperator; // select or project or scan
    private Tuple tempTuple = null; // store the current tuple of the current left operator
    private SelectExpressionDeParser selectExpressionDeParser; // extend ExpressionDeParser

    /**
     * Constructor
     *
     * @param expression    where condition
     * @param schema        schema of both tables (left and right)
     * @param leftOperator  left child
     * @param rightOperator right child
     */
    public JoinOperator(Expression expression, List<String> schema, Operator leftOperator, Operator rightOperator) {
        this.leftOperator = leftOperator;
        this.rightOperator = rightOperator;
        // create an selectExpressionDeParser if where clause exists
        if (expression != null) {
            selectExpressionDeParser = new SelectExpressionDeParser(expression, schema);
        }
    }

    /**
     * Check where condition of left table and right table
     * Use double loop
     * If check passed, return the tuple
     *
     * @return next tuple
     */
    @Override
    public Tuple getNextTuple() {
        Tuple leftTuple; // tuple of left table

        // go through left table to find the matching
        while ((leftTuple = tempTuple == null ? leftOperator.getNextTuple() : tempTuple) != null) {
            outerLoopCount++;
            Tuple rightTuple;

            while ((rightTuple = rightOperator.getNextTuple()) != null) {
                innerLoopCount++;
                if (selectExpressionDeParser != null) { // if where expression exists
                    // check is the element from left table and right table meets the requirement
                    if (selectExpressionDeParser.checkTuple(Tuple.add(leftTuple, rightTuple))) {
                        tempTuple = leftTuple;
                        return Tuple.add(leftTuple, rightTuple); // combine the elements from left table and right table
                    }
                } else { // no where condition
                    tempTuple = leftTuple;
                    return Tuple.add(leftTuple, rightTuple); // combine and return tuple
                }
            }
            rightOperator.reset();
            tempTuple = null;
        }
        return null;
    }

    /**
     * Call its child's reset method
     */
    @Override
    public void reset() {
        this.leftOperator.reset();
    }

    /**
     * Convert tuple that an int array into string and store it in file
     *
     * @param tuple data to output
     */
    @Override
    public void dump(Tuple tuple) throws IOException {

    }
}
