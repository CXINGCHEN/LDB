package ed.inf.adbs.lightdb.Utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

/**
 * Catalog
 */
public class DatabaseCatalog {

    private String databaseDir = null;
    private Map<String, List<String>> schemaMap = new HashMap<>();  // {tableName, schema}
    private volatile static DatabaseCatalog sInstance;

    /**
     * Private constructor
     */
    private DatabaseCatalog() {
    }

    /**
     * Singleton pattern
     *
     * @return static instance
     */
    public static DatabaseCatalog getsInstance() {
        if (sInstance == null) {
            synchronized (DatabaseCatalog.class) {
                if (sInstance == null) {
                    sInstance = new DatabaseCatalog();
                }
            }
        }
        return sInstance;
    }

    /**
     * Need to be called at the start of processing a sql
     * Store the path to the database
     *
     * @param databaseDir
     */
    public void setDatabaseDir(String databaseDir) {
        this.databaseDir = databaseDir;
    }

    /**
     * Get table file path
     *
     * @param tableName to be processes
     * @return file path of the table
     */
    public String getFileByTableName(String tableName) {
        String tablePath = null;
        if (this.databaseDir != null) {
            tablePath = this.databaseDir + "/data/" + tableName.split(" ")[0] + ".csv";
        }
        return tablePath;
    }

    /**
     * check if Alias is used
     *
     * @param plainSelect sql
     * @return true if Alias is used, false if not
     */
    public boolean ifUseAlias(PlainSelect plainSelect) {
        boolean ifAliasUsed;
        if (plainSelect.getFromItem().getAlias() != null) {
            ifAliasUsed = true;
        } else {
            ifAliasUsed = false;
        }
        return ifAliasUsed;
    }

    /**
     * Process Aliases, change schema to schema with aliases
     *
     * @param schema original schema
     * @param alias
     */
    public void convertToAliasSchema(List<String> schema, String alias) {
        if (!schema.get(0).contains(".")) {
            for (int i = 0; i < schema.size(); i++) {
                schema.set(i, alias + "." + schema.get(i));
            }
        }
    }

    /**
     * Get column of a table
     *
     * @param plainSelect sql
     * @return schema list
     */
    public List<String> getSchemas(PlainSelect plainSelect) {
        List<String> schemaList = new ArrayList<>();
        for (String tableName : getTableNameList(plainSelect)) {
            List<String> schema = getSchemaByTableName(tableName);
            if (ifUseAlias(plainSelect)) {
                convertToAliasSchema(schema, tableName.split(" ")[1]);
            }
            schemaList.addAll(schema);
        }
        return schemaList;
    }

    public boolean isInteger(String s) {
        return s.matches("\\d+");
    }

    /**
     * split where condition
     *
     * @param expression where condition
     * @return tabel name list
     */
    public List<String> getTableName(Expression expression) {
        List<String> tableNameList = new ArrayList<>();

        String[] split = expression.toString().split("AND"); //split expression

        for (String s : split) {
            String left = s.split("= | != | > | >= | < | <=")[0].trim(); //split expression
            String right = s.split("= | != | > | >= | < | <=")[1].trim();//split expression

            if (!isInteger(left)) {
                String tableName = left.split("\\.")[0];//split expression get alias
                if (!tableNameList.contains(tableName)) {
                    tableNameList.add(left.split("\\.")[0]);// remove duplicate name and add name in to the name list
                }
            }

            if (!isInteger(right)) {
                String tableName = right.split("\\.")[0];//split expression get alias
                if (!tableNameList.contains(tableName)) {
                    tableNameList.add(right.split("\\.")[0]);// remove duplicate name and add name in to the name list
                }
            }
        }
        return tableNameList;
    }

    /**
     * get table name by SQL, use getFromItem to get the first table name,
     * use getJoins to get the rest of the table name
     *
     * @param plainSelect sql
     * @return tabel name list
     */
    public List<String> getTableNameList(PlainSelect plainSelect) {
        List<String> tableNameList = new ArrayList<>();
        String tableName = plainSelect.getFromItem().toString(); // get the first table name
        List<Join> joins = plainSelect.getJoins();
        tableNameList.add(tableName);
        if (joins != null) {
            for (Join join : joins) {
                tableNameList.add(join.toString());// get the rest table names
            }
        }
        return tableNameList;
    }

    /**
     * read from schema.txt
     *
     * @param tableName name of a table
     * @return columns of the table
     */
    public List<String> getSchemaByTableName(String tableName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(databaseDir + "/schema.txt"));
            String line;
            while ((line = reader.readLine()) != null) {
                List<String> list = Arrays.asList(line.trim().split(" "));// split
                schemaMap.put(list.get(0), list.subList(1, list.size())); // table name and columns
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return schemaMap.get(tableName.split(" ")[0]);
    }

    /**
     * transfer list condition into expression type
     *
     * @param conditions list
     * @return expression
     */
    public Expression mergeExpressionWithAnd(List<Expression> conditions) {
        Expression expression = null;

        if (conditions.size() == 1) {
            expression = conditions.get(0);
        }

        if (conditions.size() >= 2) {
            for (int i = 0; i < conditions.size() - 1; i++) {
                Expression front = i == 0 ? conditions.get(i) : expression;
                Expression rear = conditions.get(i + 1);
                expression = new AndExpression(front, rear);
            }
        }

        return expression;
    }

    public Expression getEligibleExpression(List<String> left, String right, List<String> tableNameList, List<Expression> joinConditionList) {

        List<Expression> expressionList = new ArrayList<>();

        for (Expression expression : joinConditionList) {
            List<String> list = getSortedTableNameFromExpression(expression, tableNameList);

            for (String string : left) {
                String[] l = string.split(" ");
                String[] r = right.split(" ");
                if (l[l.length - 1].equals(list.get(0)) && r[r.length - 1].equals(list.get(1))) {
                    expressionList.add(expression);
                    break;
                }
            }
        }
        System.out.println("conditions: " + expressionList);

        Expression expression = mergeExpressionWithAnd(expressionList);

        System.out.println("mergeExpressionWithAnd: " + expression);
        return expression;
    }

    public List<String> getSortedTableNameFromExpression(Expression expression, List<String> tableNameList) {
        List<String> tableName = getTableName(expression);
        System.out.println("tableNameList: " + tableNameList);
        List<String> l = new ArrayList<>(tableNameList);
        System.out.println("l: " + tableNameList);
        for (int i = 0; i < tableNameList.size(); i++) {
            String[] split = tableNameList.get(i).split(" ");
            l.set(i, split[split.length - 1]);
        }
        if (l.indexOf(tableName.get(0)) > l.indexOf(tableName.get(1))) {
            String temp = tableName.get(0);
            tableName.set(0, tableName.get(1));
            tableName.set(1, temp);
        }
        return tableName;
    }


    public PlainSelect addOrderBy(PlainSelect plainSelect) {

        System.out.println("原始：" + plainSelect.toString());
        PlainSelect res = null;
        StringBuilder orderBy = new StringBuilder();

        if (plainSelect.getSelectItems().get(0).toString().contains("*")) {
            for (String s : getSchemas(plainSelect)) {
                orderBy.append(s + ",");
            }
        } else {
            for (SelectItem selectItem : plainSelect.getSelectItems()) {
                orderBy.append(selectItem.toString() + ",");
            }
        }

        String sql;
        if (plainSelect.getOrderByElements() == null) {
            sql = plainSelect + " ORDER BY " + orderBy.substring(0, orderBy.length() - 1);
        } else {
            String ss = "";
            for (String s : orderBy.toString().split(",")) {
                Boolean flag = true;
                for (OrderByElement orderByElement : plainSelect.getOrderByElements()) { // S.B
                    if (s.equals(orderByElement.toString())) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    ss += s + ",";
                }
            }
            sql = plainSelect + "," + ss.substring(0, ss.length() - 1);
        }
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            Select select = (Select) statement;
            res = (PlainSelect) select.getSelectBody();
            System.out.println("NEW：" + res.toString());
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }

        return res;
    }

}
