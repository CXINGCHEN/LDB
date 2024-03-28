package ed.inf.adbs.lightdb.Operator;

import ed.inf.adbs.lightdb.Utils.DatabaseCatalog;
import ed.inf.adbs.lightdb.Utils.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * GROUP BY & SUM Aggregation
 */
public class SumOperator extends Operator {

    private Operator operator; // child
    private BufferedWriter writer;
    private int sumMultiply; // sum number
    private int sum = 0; // total sum value
    private Map<Tuple, Integer> sumMap; // group by
    private List<Tuple> resultList; // buffer
    private String sumColumName; // selected column for sum
    private String tableName;
    private PlainSelect plainSelect; // sql
    private List<String> groupByColumList; // column name for group by
    private List<String> schema;
    boolean outputGroupByColum = false;

    /**
     * Constructor
     * @param plainSelect sql
     * @param tableName name of table
     * @param operator child
     * @param sumMultiply sum number
     * @param sumColumName selected column for sum
     * @param groupByColumList column name for group by
     * @param outputFile output path
     */
    public SumOperator(PlainSelect plainSelect, String tableName, Operator operator, int sumMultiply, String sumColumName, List<String> groupByColumList, String outputFile) {
        this.plainSelect = plainSelect;
        this.tableName = tableName;
        this.operator = operator;
        this.sumMultiply = sumMultiply;
        this.sumColumName = sumColumName;
        this.groupByColumList = groupByColumList;
        this.sum = 0;
        this.sumMap = new HashMap<>();
        this.resultList = new ArrayList<>();

        this.schema = DatabaseCatalog.getsInstance().getSchemaByTableName(tableName); // get schema

        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<SelectItem> selectItems = plainSelect.getSelectItems();
        System.out.println("SumOperator selectItems = " + selectItems);
        System.out.println("SumOperator groupByColumList = " + groupByColumList);

        // check group by
        for (SelectItem selectItem : selectItems) {
            for (String groupByColum : groupByColumList) {
                if(selectItem.toString().equals(groupByColum)) {
                    outputGroupByColum = true;
                    break;
                }
            }
        }

        System.out.println("SumOperator outputGroupByColum = " + outputGroupByColum);

        if (sumMultiply > 0 || !sumColumName.isEmpty()) {
            // has sum
            getSumList();
        } else {
            // no sum but group by
            getGroupByTupleList();
        }
    }

    /**
     * Group by without sum
     */
    private void getGroupByTupleList() {

        if (tableName.split(" ").length == 2) {
            DatabaseCatalog.getsInstance().convertToAliasSchema(schema, tableName.split(" ")[1]);
        }

        System.out.println("sumColumName==============" + sumColumName);
        System.out.println("groupByColumList==============" + groupByColumList); // [Sailors.B, Sailors.C]
        System.out.println("schema==============" + schema);

        List<Integer> groupByColumIndexList = new ArrayList<>();

        for (String s : groupByColumList) {
            String[] groupBySplit = s.split("\\.");
            String groupByColumName = groupBySplit[1];
            System.out.println("groupByColum indexOf==========" + schema.indexOf(groupByColumName));
            groupByColumIndexList.add(schema.indexOf(groupByColumName));
        }
        Tuple current;
        while ((current = this.operator.getNextTuple()) != null) {
            int[] value = new int[groupByColumIndexList.size()];
            for (int j = 0; j < groupByColumIndexList.size(); j++) {
                value[j] = current.get(groupByColumIndexList.get(j));
            }
            Tuple tuple = new Tuple(value);
            if (!resultList.contains(tuple)) {
                resultList.add(tuple);
            }
        }

        List<Integer> order = new ArrayList<>();

        for (OrderByElement orderByElement : this.plainSelect.getOrderByElements()) {
            Column column = (Column) orderByElement.getExpression();
            Integer index = getIndexByColumName(column.toString());
            order.add(groupByColumIndexList.indexOf(index));
        }

        System.out.println("Order By Index is: " + order);
        System.out.println("Group By Index is: " + groupByColumIndexList);

        Collections.sort(this.resultList, (o1, o2) -> {
            int temp = 0;
            for (Integer index : order) {
                if ((temp = o1.get(index) - o2.get(index)) != 0) {
                    return temp;
                }
            }
            return temp;
        });
    }

    /**
     * Sum
     */
    private void getSumList() {

        if (tableName.split(" ").length == 2) {
            DatabaseCatalog.getsInstance().convertToAliasSchema(schema, tableName.split(" ")[1]);
        }

        System.out.println("sumColumName==============" + sumColumName);
        System.out.println("groupByColumList==============" + groupByColumList); // Sailors.B
        System.out.println("schema==============" + schema);


        List<Integer> groupByColumIndexList = new ArrayList<>();

        for (String s : groupByColumList) {
            String[] groupBySplit = s.split("\\.");
            String groupByColumName = groupBySplit[1];
            groupByColumIndexList.add(schema.indexOf(groupByColumName));
        }

        System.out.println("groupByColumIndexList==============" + groupByColumIndexList);

        int columIndex = -1;
        int leftIndex = -1;
        int rightIndex = -1;
        if (!sumColumName.isEmpty() && sumColumName.contains("*") || sumColumName.contains("+")) {
            String[] split = sumColumName.split("\\*");
            String columLeft = split[0].trim();
            String columRight = split[1].trim();
            leftIndex = getIndexByColumName(columLeft);
            rightIndex = getIndexByColumName(columRight);
        } else if (!sumColumName.isEmpty()) {
            columIndex = getIndexByColumName(sumColumName.trim());
        }

        Tuple current;
        while ((current = this.operator.getNextTuple()) != null) {
            int tmpSum = 0;
            if (sumColumName.isEmpty()) {
                tmpSum = sumMultiply;
            } else if (sumColumName.contains("*")) {
                tmpSum = current.get(leftIndex) * current.get(rightIndex);
            } else if (sumColumName.contains("+")) {
                tmpSum = current.get(leftIndex) + current.get(rightIndex);
            } else {
                tmpSum = current.get(columIndex);
            }

            if (groupByColumList.isEmpty()) {
                sum += tmpSum;
            } else {

                int[] arr = new int[groupByColumIndexList.size()];
                for (int i = 0; i < groupByColumIndexList.size(); i++) {
                    int index = groupByColumIndexList.get(i);
                    arr[i] = current.get(index);
                }

                Tuple tuple = new Tuple(arr);
                if (sumMap.containsKey(tuple)) {
                    Integer i1 = sumMap.get(tuple);
                    sumMap.put(tuple, i1 + tmpSum);
                } else {
                    sumMap.put(tuple, tmpSum);
                }
            }
        }

        if (sumMap.isEmpty()) {
            resultList.add(new Tuple(new int[]{sum}));
        } else {
            sumMap.forEach((key, value) -> {
                if(outputGroupByColum) {
                    resultList.add(Tuple.add(key,new Tuple(new int[]{value})));
                } else {
                    resultList.add(new Tuple(new int[]{value}));
                }
            });
        }
    }

    /**
     * @param columName schema
     * @return index
     */
    private int getIndexByColumName(String columName) {
        String[] split = columName.split("\\.");
        String s = split[1].trim();
        return schema.indexOf(s);
    }

    int getTupleIndex = 0;

    /**
     * @return next tuple
     */
    @Override
    public Tuple getNextTuple() {
        if (getTupleIndex < resultList.size()) {
            Tuple tuple = resultList.get(getTupleIndex);
            getTupleIndex++;
            return tuple;
        }
        return null;
    }

    /**
     * Call its child's reset method
     */
    public void reset() {
        operator.reset();
    }

    /**
     * Convert tuple that an int array into string and store it in file
     *
     * @param tuple data to output
     */
    public void dump(Tuple tuple) throws IOException {
        try {
            writer.write(tuple.toString());
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
