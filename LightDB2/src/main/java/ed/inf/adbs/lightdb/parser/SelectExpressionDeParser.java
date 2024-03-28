package ed.inf.adbs.lightdb.parser;

import ed.inf.adbs.lightdb.Utils.DatabaseCatalog;
import ed.inf.adbs.lightdb.Utils.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.List;

/**
 * Input: expression and schema
 */
public class SelectExpressionDeParser extends ExpressionDeParser {

    private Expression expression; // where condition
    private List<String> schema; // schema of table
    private Tuple tuple; // 1 row of data, int array
    private List<Boolean> checkResultList = new ArrayList<>();  // store the result

    /**
     * Constructor
     *
     * @param expression where condition
     * @param schema     schema of table
     */
    public SelectExpressionDeParser(Expression expression, List<String> schema) {
        this.expression = expression;
        this.schema = schema;
    }

    /**
     * Check if this tuple meet all the where requirements
     * @param tuple
     * @return true or false depends on if matches or not
     */
    public boolean checkTuple(Tuple tuple) {
        checkResultList.clear();
        this.tuple = tuple;
        expression.accept(this); // call visit method
        this.tuple = null;

        boolean match = true; // default true
        for (boolean b : checkResultList) {
            if (!b) { // if any false occured, set to false
                match = false;
                break;
            }
        }
        return match;
    }

    private void compare(ComparisonOperator expression, String operator) {
        Expression leftElement = expression.getLeftExpression();
        Expression rightElement = expression.getRightExpression();

        // check is right hand side or left hand side is a number
        boolean isLeftNumber = DatabaseCatalog.getsInstance().isInteger(leftElement.toString());
        boolean isRightNumber = DatabaseCatalog.getsInstance().isInteger(rightElement.toString());

        // check alias
        boolean useAliases = schema.get(0).split("\\.").length == 2;

        int leftValue;
        int rightValue;
        
        if (isLeftNumber) { // directly transfer to int
            leftValue = Integer.parseInt(leftElement.toString());
        } else {
            Column column = (Column) leftElement;
            int index = useAliases ? schema.indexOf(column.toString()) : schema.indexOf(column.getColumnName());
            leftValue = tuple.get(index); // get number from tuple by index
        }

        if(isRightNumber) { // directly transfer to int
            rightValue = Integer.parseInt(rightElement.toString());
        } else {
            Column column = (Column) rightElement;
            int index = useAliases ? schema.indexOf(column.toString()) : schema.indexOf(column.getColumnName());
            rightValue = tuple.get(index); // get number from tuple by index
        }

        switch (operator) {
            case "=":
                checkResultList.add(leftValue == rightValue);
                break;
            case "!=":
                checkResultList.add(leftValue != rightValue);
                break;
            case "<":
                checkResultList.add(leftValue < rightValue);
                break;
            case "<=":
                checkResultList.add(leftValue <= rightValue);
                break;
            case ">":
                checkResultList.add(leftValue > rightValue);
                break;
            case ">=":
                checkResultList.add(leftValue >= rightValue);
                break;
        }
    }


    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);
        compare(equalsTo, "=");
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        super.visit(notEqualsTo);
        compare(notEqualsTo, "!=");
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        compare(greaterThan, ">");
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        compare(greaterThanEquals, ">=");
    }

    @Override
    public void visit(MinorThan minorThan) {
        super.visit(minorThan);
        compare(minorThan, "<");
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        compare(minorThanEquals, "<=");
    }

}

