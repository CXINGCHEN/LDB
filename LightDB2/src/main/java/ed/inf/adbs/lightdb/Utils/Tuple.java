package ed.inf.adbs.lightdb.Utils;

import java.util.Arrays;
import java.util.List;

/**
 * int tuple class
 */
public class Tuple {

    private int[] tuple;

    public Tuple(int[] tuple) {
        this.tuple = tuple;
    }

    public int[] getTuple() { // get method
        return tuple;
    }

    public void setTuple(int[] tuple) { // set method
        this.tuple = tuple;
    }

    public int get(int index) { // get element of tuple by index
        return tuple[index];
    }

    public static Tuple add(Tuple left, Tuple right) {
        int[] leftArray = left.tuple;
        int[] rightArray = right.tuple;

        // Creates a new array with the length of the sum of the two original arrays
        int[] resultArray = new int[leftArray.length + rightArray.length];

        // Copy the left array to the new array
        System.arraycopy(leftArray, 0, resultArray, 0, leftArray.length);

        // Copy the right array to the new array
        System.arraycopy(rightArray, 0, resultArray, leftArray.length, rightArray.length);

        return new Tuple(resultArray);
    }

    /**
     *
     * @param object
     * @return
     */
    @Override
    public boolean equals(Object object) {
        int[] l1 = this.getTuple();
        int[] l2 = ((Tuple) object).getTuple();
        if (l1.length == l2.length) {
            for (int i = 0; i < l1.length; i++) {
                if (!(l1[i] == l2[i])) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString() {
        String s = Arrays.toString(tuple);
        return s.substring(1, s.length() - 1);
    }
}
