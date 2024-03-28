package ed.inf.adbs.lightdb.Operator;

import ed.inf.adbs.lightdb.Utils.Tuple;

import java.io.*;

/**
 * Support queries for full table scans, e.g., SELECT * FROM Sailors
 * Child class of the Operator class
 */
public class ScanOperator extends Operator {

    private String tablePath; // store the path of the table to be processed
    private BufferedReader reader = null; // read from file
    private BufferedWriter writer = null; // write to file

    /**
     * Constructor
     *
     * @param tablePath  path to the table to be read
     * @param outputFile path to the output file that contain the result of the query
     */
    public ScanOperator(String tablePath, String outputFile) {
        this.tablePath = tablePath;
        try {
            reader = new BufferedReader(new FileReader(tablePath));
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * if readLine() == NULL return null, or return next tuple
     *
     * @return 1 tuple in int array format or null
     */
    @Override
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine(); // read 1 row of the table
            if (line == null || line.trim().isEmpty()) {
                return null;
            }
            String[] split = line.split(","); // string array "split" contains the data of each row
            int[] tupleArray = new int[split.length]; // create an int array, length = the length of the string array "split"

            // Transfer each string in the "split" array into integer and store it into the int array “tupleArray”
            for (int i = 0; i < split.length; i++) {
                tupleArray[i] = Integer.parseInt(split[i].trim());
            }

            return new Tuple(tupleArray);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Create a new reader using the same table path
     */
    @Override
    public void reset() {
        try {
            this.reader = new BufferedReader(new FileReader(tablePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Convert tuple that an int array into string and store it in file
     *
     * @param tuple data to output
     */
    @Override
    public void dump(Tuple tuple) {
        try {
            writer.write(tuple.toString());
            writer.newLine(); // \n
            writer.flush(); // memory to disk
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
