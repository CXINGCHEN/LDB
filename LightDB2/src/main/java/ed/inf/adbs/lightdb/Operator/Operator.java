package ed.inf.adbs.lightdb.Operator;

import ed.inf.adbs.lightdb.Utils.Tuple;

import java.io.IOException;

/**
 * Abstract class for operator
 */
public abstract class Operator {

    /**
     * Constructor
     */
    public Operator() {
    }

    /**
     * Abstract method for getNextTuple()
     *
     * @return next tuple / data row
     */
    public abstract Tuple getNextTuple();

    /**
     * Abstract method for reset()
     * Go back to the start of the table / restart
     */
    public abstract void reset();

    /**
     * Write 1 data row / 1 tuple into file
     *
     * @param tuple data to output
     * @throws IOException
     */
    public abstract void dump(Tuple tuple) throws IOException;
}
