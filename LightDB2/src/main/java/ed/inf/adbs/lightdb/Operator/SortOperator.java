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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ORDER BY
 */
public class SortOperator extends Operator {

    private PlainSelect plainSelect; // sql
    private Operator operator; // any type of child operator
    private List<Tuple> resultList = new ArrayList<>(); // internal buffer to store all output from child for sorting
    private Integer getTupleIndex = 0;
    private BufferedWriter writer;

    /**
     * Constructor
     *
     * @param plainSelect sql
     * @param operator child operator
     * @param outputFile path for result file
     */
    public SortOperator(PlainSelect plainSelect, Operator operator, String outputFile) {
        this.plainSelect = plainSelect;
        this.operator = operator;
        try {
            writer = new BufferedWriter(new FileWriter(outputFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @return next tuple
     */
    @Override
    public Tuple getNextTuple() {
        if (plainSelect.getOrderByElements() == null) { // if doesn't has order by
            return operator.getNextTuple();
        }

        if (resultList.isEmpty()) {
            boolean useAliases = DatabaseCatalog.getsInstance().ifUseAlias(this.plainSelect);

            List<SelectItem> selectItemList = plainSelect.getSelectItems();
            List<String> selectItems = new ArrayList<>();

            if (selectItemList.get(0).toString().equals("*")) { // if select * add all schema
                selectItems = DatabaseCatalog.getsInstance().getSchemas(plainSelect);
            } else { // if not select * only add chosen column
                for (SelectItem selectItem : selectItemList) {
                    if (useAliases) {
                        selectItems.add(selectItem.toString());
                    } else {
                        selectItems.add(selectItem.toString().split("\\.")[1]);
                    }
                }
            }

            List<Integer> order = new ArrayList<>();
            for (OrderByElement orderByElement : plainSelect.getOrderByElements()) {
                Column column = (Column) orderByElement.getExpression();
                Integer index = useAliases ? selectItems.indexOf(column.toString()) : selectItems.indexOf(column.getColumnName());
                order.add(index);
            }

            Tuple tuple;
            while ((tuple = operator.getNextTuple()) != null) {
                resultList.add(tuple);
            }

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

        if(getTupleIndex < resultList.size()){
            return resultList.get(getTupleIndex++);
        }
        return null;
    }

    /**
     * Call its child's reset method
     */
    @Override
    public void reset() {
        this.operator.reset();
        this.getTupleIndex = 0;
    }

    /**
     * Convert tuple that an int array into string and store it in file
     *
     * @param tuple data to output
     */
    @Override
    public void dump(Tuple tuple) {
        try {
            writer.write(String.valueOf(tuple));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
