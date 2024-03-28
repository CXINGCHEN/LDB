package ed.inf.adbs.lightdb.parser;

import ed.inf.adbs.lightdb.Utils.DatabaseCatalog;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.ArrayList;
import java.util.List;

/**
 * get select condition and join condition
 */
public class JoinExpressionDeParser extends ExpressionDeParser {

    private List<Expression> joinExpressionList = new ArrayList<>(); // double table expressions
    private List<Expression> selectionExpressionList = new ArrayList<>(); // single table expressions

    /**
     * check if one of the right hand side or the left hand side is a number
     * if ture, this is a single table condition
     * if both sides are table name, check if the RHS and the LHS table name is the same
     * if they are the same, it is a single table condition, use select operator to process it
     * if not the same, this condition requires 2 tables, use join
     *
     * @param leftElement  right hand side of the expression
     * @param rightElement left hand side of the expression
     * @return if it is join that require 2 tables
     */
    public static boolean isJoin(String leftElement, String rightElement) { // right hand side and left hand side of the expression
        if (DatabaseCatalog.getsInstance().isInteger(leftElement) || DatabaseCatalog.getsInstance().isInteger(rightElement)) {
            return false;
        }
        if (leftElement.split("\\.")[0].equals(rightElement.split("\\.")[0])) {
            return false;
        }
        return true;
    }

    /**
     * add expression to join condition list or select condition list
     *
     * @param expression where condition
     */
    private void extractJoin(ComparisonOperator expression) {
        System.out.println("expression: " + expression);
        String leftElement = expression.getLeftExpression().toString();
        String rightElement = expression.getRightExpression().toString();
        System.out.println("leftElement: " + leftElement);
        System.out.println("rightElement: " + rightElement);

        if (isJoin(leftElement, rightElement)) {
            joinExpressionList.add(expression);
        } else {
            selectionExpressionList.add(expression);
        }
    }

    /**
     * @return Join Expression List (2 tables condition)
     */
    public List<Expression> getJoinExpressionList() {
        return joinExpressionList;
    }

    /**
     * @return Selection Expression List (single table condition)
     */
    public List<Expression> getSelectionExpressionList() {
        return selectionExpressionList;
    }

    @Override
    public void visit(EqualsTo equalsTo) {
        super.visit(equalsTo);
        extractJoin(equalsTo);
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {
        super.visit(notEqualsTo);
        extractJoin(notEqualsTo);
    }

    @Override
    public void visit(GreaterThan greaterThan) {
        super.visit(greaterThan);
        extractJoin(greaterThan);
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {
        super.visit(greaterThanEquals);
        extractJoin(greaterThanEquals);
    }

    @Override
    public void visit(MinorThan minorThan) {
        super.visit(minorThan);
        extractJoin(minorThan);
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {
        super.visit(minorThanEquals);
        extractJoin(minorThanEquals);
    }
}