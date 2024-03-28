package ed.inf.adbs.lightdb;

import ed.inf.adbs.lightdb.Operator.*;
import ed.inf.adbs.lightdb.Utils.DatabaseCatalog;
import ed.inf.adbs.lightdb.parser.JoinExpressionDeParser;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Get input file and output file
 * Create plan and organise operator
 */
public class Planner {

    private String inputFile;
    private String outputFile;

    /**
     * constructor
     *
     * @param inputFile  sql
     * @param outputFile result path
     */
    public Planner(String inputFile, String outputFile) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    /**
     * analyse sql and set up execution plan
     *
     * @return Operator
     */
    public Operator createPlan() {
        try {
            Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));

            if (statement != null) {
                System.out.println("Read statement: " + statement);

                Select select = (Select) statement;//select class is the child of statement class
                System.out.println("Select: " + select);

                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
                System.out.println("Plain Select body is " + plainSelect);

                //table name
                FromItem fromItem = plainSelect.getFromItem();

                Table table = (Table) fromItem;
                String tableName = table.getName();

                System.out.println("tableName: " + tableName);
                System.out.println("Columns: " + plainSelect.getSelectItems());
                System.out.println("From: " + plainSelect.getFromItem());
                System.out.println("Where: " + plainSelect.getWhere());

                List<Join> plainSelectJoins = plainSelect.getJoins();
                System.out.println("Joins: " + plainSelectJoins);

                final boolean[] hasSum = {false};
                final int[] sumNumber = {0};
                final String[] sumColumn = {""};

                List<String> groupByColumnlist = new ArrayList<>();

                plainSelect.getSelectItems().forEach(item -> {
                    System.out.println("SelectItem: " + item.toString());
                    item.accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(Function function) {
                            super.visit(function);
                            if ("SUM".equalsIgnoreCase(function.getName())) {
                                hasSum[0] = true;
                                System.out.println("Found SUM column: " + function);
                                Expression expression = function.getParameters().getExpressions().get(0);
                                System.out.println("SUM operand: " + expression.toString()); // Reserves.G * Reserves.G
                                if (DatabaseCatalog.getsInstance().isInteger(expression.toString())) {
                                    sumNumber[0] = Integer.parseInt(expression.toString());
                                } else {
                                    sumColumn[0] = expression.toString();
                                }
                            }
                        }
                    });
                });

                if (plainSelect.getGroupBy() != null) {
                    List<Expression> groupByExpressions = plainSelect.getGroupBy().getGroupByExpressions();
                    if (groupByExpressions != null) {
                        for (Expression expression : groupByExpressions) {
                            System.out.println("Group By column: " + expression.toString());
                            groupByColumnlist.add(expression.toString());
                        }
                    }
                }

                System.out.println("hasSum: " + hasSum[0]);
                System.out.println("sumNumber: " + sumNumber[0]);
                System.out.println("sumColumn: " + sumColumn[0]);

                boolean ifHasJoin = plainSelect.getJoins() != null;

                System.out.println("ifHasJoin: " + ifHasJoin + ", " + plainSelect.getJoins());

                Operator sqlPlan;
                if (!ifHasJoin) { // if doesn't contain join, just use select
                    sqlPlan = new SelectOperator(plainSelect.getWhere(), tableName, outputFile);
                } else {
                    JoinExpressionDeParser joinExpressionDeParser = new JoinExpressionDeParser();

                    Expression where = plainSelect.getWhere(); // where expression
                    if (where != null) {
                        where.accept(joinExpressionDeParser); // call visit method
                    }

                    List<String> tableList = DatabaseCatalog.getsInstance().getTableNameList(plainSelect); // join contains multiple lists
                    Map<String, Operator> selectionMap = new HashMap<>(); // there are multiple tables that may have multiple single table conditions

                    for (String string : tableList) {
                        List<Expression> conditions = new ArrayList<>();
                        System.out.println("joinExpressionDeParser.getSelectionExpressionList() : " + joinExpressionDeParser.getSelectionExpressionList());
                        for (Expression expression : joinExpressionDeParser.getSelectionExpressionList()) {
                            // for all single table condition
                            List<String> names = DatabaseCatalog.getsInstance().getTableName(expression);
                            if (names.size() == 0) {
                                if (string.equals(plainSelect.getFromItem().toString())) {
                                    conditions.add(expression);
                                }
                            } else {
                                String[] split = string.split(" ");
                                if (split[split.length - 1].equals(names.get(0))) {
                                    conditions.add(expression);
                                }
                            }
                        }
                        Expression expression = DatabaseCatalog.getsInstance().mergeExpressionWithAnd(conditions);
                        System.out.println("mergeExpressionWithAnd: " + expression);

                        SelectOperator selectOperator = new SelectOperator(expression, string, outputFile);
                        selectionMap.put(string, selectOperator);
                    }

                    JoinOperator previousOperator = null;
                    List<String> previousSchema = null;

                    for (int i = 0; i < tableList.size() - 1; i++) {
                        System.out.println("tableName: " + tableList.get(i));
                        Operator leftOperator;
                        Operator rightOperator;
                        List<String> leftTables = tableList.subList(0, i + 1); // left table
                        String rightTables = tableList.get(i + 1); // right table

                        List<String> schema = new ArrayList<>();// Get schema
                        List<String> leftSchema = previousSchema == null ? DatabaseCatalog.getsInstance().getSchemaByTableName(leftTables.get(leftTables.size() - 1)) : previousSchema;
                        List<String> rightSchema = DatabaseCatalog.getsInstance().getSchemaByTableName(rightTables);

                        if (DatabaseCatalog.getsInstance().ifUseAlias(plainSelect)) { // check aliases
                            DatabaseCatalog.getsInstance().convertToAliasSchema(leftSchema, leftTables.get(leftTables.size() - 1).split(" ")[1]);
                            DatabaseCatalog.getsInstance().convertToAliasSchema(rightSchema, rightTables.split(" ")[1]);
                        }
                        schema.addAll(leftSchema);
                        schema.addAll(rightSchema);

                        System.out.println("joinExpressionDeParser.getJoinExpressionList() : " + joinExpressionDeParser.getJoinExpressionList());
                        Expression expression = DatabaseCatalog.getsInstance().getEligibleExpression(leftTables, rightTables, tableList, joinExpressionDeParser.getJoinExpressionList());

                        // The first loop is of type SelectOperator and the second loop is of type JoinOperator
                        leftOperator = previousOperator == null ? selectionMap.get(leftTables.get(leftTables.size() - 1)) : previousOperator;
                        rightOperator = selectionMap.get(rightTables);

                        previousOperator = new JoinOperator(expression, schema, leftOperator, rightOperator);
                        previousSchema = schema;
                    }
                    sqlPlan = previousOperator;
                }

                if (hasSum[0]) {
                    return new SumOperator(plainSelect, tableName, sqlPlan, sumNumber[0], sumColumn[0], groupByColumnlist, outputFile);
                } else if (!groupByColumnlist.isEmpty()) {
                    return new SumOperator(plainSelect, tableName, sqlPlan, -1, "", groupByColumnlist, outputFile);
                }

                ProjectOperator projectOperator = new ProjectOperator(plainSelect, sqlPlan, outputFile);

                SortOperator sortOperator = new SortOperator(plainSelect, projectOperator, outputFile);

                if (plainSelect.getDistinct() != null) {
                    sortOperator = new SortOperator(DatabaseCatalog.getsInstance().addOrderBy(plainSelect), projectOperator, outputFile);
                }

                return new DuplicateEliminationOperator(plainSelect, sortOperator, outputFile);
            }
        } catch (Exception e) {
            System.err.println("Exception occurred during parsing");
            e.printStackTrace();
        }
        return null;
    }
}
