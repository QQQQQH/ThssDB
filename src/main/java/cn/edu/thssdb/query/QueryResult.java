package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.Statement.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;
import javafx.scene.control.Cell;

public class QueryResult {

    private List<MetaInfo> metaInfos;
    private List<Integer> index;
    private List<String> attrs;
    private List<QueryTable> queryTables;
    private List<List<String>> resultRowList;

    public QueryResult(ArrayList<Table> tables2Query) {
        // TODO
        index = null;
        attrs = null;
        resultRowList = null;
        resultRowList = null;
        queryTables = new ArrayList<>();
        metaInfos = new ArrayList<>();
        for (Table table: tables2Query) {
            queryTables.add(new QueryTable(table));
            metaInfos.add(new MetaInfo(table.tableName, table.columns));
        }
    }

    public QueryResult(Table table) {
        index = null;
        attrs = null;
        resultRowList = null;
        resultRowList = null;
        queryTables = new ArrayList<>();
        metaInfos = new ArrayList<>();
        queryTables.add(new QueryTable(table));
        metaInfos.add(new MetaInfo(table.tableName, table.columns));
    }

    public ArrayList<Row> deleteQuery(DeleteStatement statement) {
        ArrayList<Row> resultRowList_ = getRowListFromTables(null);
        resultRowList_ = filterRowList(resultRowList_, statement.condition);
        return resultRowList_;
    }

    public ArrayList<Row> updateQuery(UpdateStatement statement) {
        ArrayList<Row> resultRowList_ = getRowListFromTables(null);
        resultRowList_ = filterRowList(resultRowList_, statement.condition);
        return resultRowList_;
    }

    public void selectQuery(SelectStatement statement) {
        ArrayList<Row> resultRowList_ = getRowListFromTables(statement.tableQuery.condition);
        resultRowList_ = filterRowList(resultRowList_, statement.condition);
        resultRowList_ = selectColumns(
                resultRowList_,
                statement.resultColumnNameList,
                metaInfos.get(0),
                metaInfos.size() >= 2 ? metaInfos.get(1) : null);
        generateStringResult(resultRowList_);
    }

    public List<List<String>> getResultRowList() {
        return resultRowList;
    }

    public List<String> getAttrList() {
        return attrs;
    }

    private void generateStringResult(ArrayList<Row> rowList) {
        resultRowList = new ArrayList<>();
        for (Row row: rowList) {
            List<String> rowStringList = new ArrayList<>();
            for (Entry entry: row.getEntries()) {
                rowStringList.add(entry.toString());
            }
            resultRowList.add(rowStringList);
        }
    }

    private void setAttrs(boolean all, MetaInfo metaInfoLeft, MetaInfo metaInfoRight) {
        this.attrs = new ArrayList<>();
        if (all) {
            int leftSize = metaInfoLeft.getColumnSize();
            for (int i = 0;i < leftSize;i++) attrs.add(metaInfoLeft.getColumnName(i));
            if (metaInfoRight != null) {
                int rightSize = metaInfoRight.getColumnSize();
                for (int i = 0;i < rightSize;i++) attrs.add(metaInfoRight.getColumnName(i));
            }
        }
        else {
            for (Integer idx: index) {
                if (idx < metaInfoLeft.getColumnSize()) {
                    this.attrs.add(metaInfoLeft.getColumnName(idx));
                }
                else {
                    this.attrs.add(metaInfoRight.getColumnName(idx-metaInfoLeft.getColumnSize()));
                }
            }
        }
    }

    private ArrayList<Row> selectColumns(ArrayList<Row> rowList,
                               ArrayList<ColumnFullName> columnFullNameList,
                               MetaInfo metaInfoLeft,
                               MetaInfo metaInfoRight) {
        ColumnFullName first = columnFullNameList.get(0);
        if (first.tableName == null && first.columnName == null) {
            // SELECT *
            setAttrs(true, metaInfoLeft, metaInfoRight);
            return rowList;
        }
        this.index = new ArrayList<>();
        for (ColumnFullName fullName: columnFullNameList) {
            int index = getColumnIndex(fullName, metaInfoLeft, metaInfoRight);
            if (index == -1) {
                throw new ColumnDoesNotExistException();
            }
            this.index.add(index);
        }
        setAttrs(false, metaInfoLeft, metaInfoRight);
        ArrayList<Row> rowList_ = new ArrayList<>();
        for (Row row: rowList) {
            rowList_.add(generateQueryRecord(row));
        }
        return rowList_;
    }

    private Row generateQueryRecord(Row row) {
        // TODO
        ArrayList<Entry> entryList = row.getEntries();
        ArrayList<Entry> entryList_ = new ArrayList<>();
        for (Integer idx: index) {
            entryList_.add(entryList.get(idx));
        }
        return new Row(entryList_);
    }

    private int getColumnIndex(ColumnFullName fullName, MetaInfo metaInfoLeft, MetaInfo metaInfoRight) {
        int index;
        if (fullName.tableName == null) {
            index = metaInfoLeft.columnFind(fullName.columnName);
            if (metaInfoRight != null) {
                int indexRight = metaInfoRight.columnFind(fullName.columnName);
                if (index != -1 && indexRight != -1) {
                    throw new DuplicateMatchedException();
                }
                else if (index == -1 && indexRight != -1) {
                    index = indexRight + metaInfoLeft.getColumnSize();
                }
            }
        }
        else {
            String tableLeftName = metaInfoLeft.getTableName();
            String tableRightName = metaInfoRight == null ? null : metaInfoRight.getTableName();
            if (fullName.tableName.equals(tableLeftName)
                    && !fullName.tableName.equals(tableRightName)) {
                index = metaInfoLeft.columnFind(fullName.columnName);
            }
            else if (!fullName.tableName.equals(tableLeftName)
                    && fullName.tableName.equals(tableRightName)) {
                index = metaInfoRight.columnFind(fullName.columnName);
                if (index != -1) {
                    index += metaInfoLeft.getColumnSize();
                }
            }
            else if (fullName.tableName.equals(tableLeftName)
                    && fullName.tableName.equals(tableRightName)) {
                throw new DuplicateMatchedException();
            }
            else {
                throw new TableNotExistException();
            }
        }
        return index;
    }

    public int columnFind(String columnName) {
        return metaInfos.get(0).columnFind(columnName);
    }

    public ArrayList<Row> updateRowList(ArrayList<Row> rowList, int index, Expression expression) {
        MetaInfo metaInfo = metaInfos.get(0);
        ArrayList<Row> rowList_ = new ArrayList<>();
        for (Row row: rowList) {
            Comparable newValue = calcExpression(expression, metaInfo, null, row);
            ColumnType type = metaInfo.getColumnType(index);
            boolean columnIsString = type.equals(ColumnType.STRING);
            boolean newValueIsString = newValue instanceof String;
            Row row_ = new Row();
            row_.appendEntries(row.getEntries());
            if (columnIsString ^ newValueIsString) {
                throw new OperandTypeNotMatchedException();
            }
            if (newValueIsString) {
                row_.getEntries().set(index, new Entry(newValue));
            }
            else {
                Double newValue_ = Double.valueOf(newValue.toString());
                if (type.equals(ColumnType.INT) || type.equals(ColumnType.LONG)) {
                    row_.getEntries().set(index, new Entry(newValue_.longValue()));
                }
                else {
                    row_.getEntries().set(index, new Entry(newValue_));
                }
            }
            rowList_.add(row_);
        }
        return rowList_;
    }

    private ArrayList<Row> filterRowList(ArrayList<Row> rowList, Condition condition) {
        if (condition == null) {
            return rowList;
        }
        else {
            ArrayList<Row> rowList_ = new ArrayList<>();
            for (Row row: rowList) {
                if (calcCondition(
                        condition,
                        metaInfos.get(0),
                        metaInfos.size() >= 2 ? metaInfos.get(1) : null,
                        row)) {
                    rowList_.add(row);
                }
            }
            return rowList_;
        }
    }

    private ArrayList<Row> getRowListFromTables(Condition condition) {
        ArrayList<Row> combinedRowList = new ArrayList<>();
        if (queryTables.size() == 1) {
            QueryTable queryTableLeft = queryTables.get(0);
            while (queryTableLeft.hasNext()) {
                Row row = queryTableLeft.next();
                if (condition == null || calcCondition(condition, metaInfos.get(0), null, row)) {
                    combinedRowList.add(row);
                }
            }
        }
        else if (queryTables.size() == 2) {
            QueryTable queryTableLeft = queryTables.get(0);
            QueryTable queryTableRight = queryTables.get(1);
            while (queryTableLeft.hasNext()) {
                Row rowLeft = queryTableLeft.next();
                queryTableRight.refresh();
                while (queryTableRight.hasNext()) {
                    Row rowCombined = combineRow(rowLeft, queryTableRight.next());
                    if (condition == null || calcCondition(condition, metaInfos.get(0), metaInfos.get(1), rowCombined)) {
                        combinedRowList.add(rowCombined);
                    }
                }
            }
        }
        return combinedRowList;
    }

    private static Row combineRow(Row rowLeft, Row rowRight) {
        // TODO
        Row rowCombined = new Row();
        rowCombined.appendEntries(rowLeft.getEntries());
        rowCombined.appendEntries(rowRight.getEntries());
        return rowCombined;
    }

    private boolean calcCondition(Condition condition,
                                  MetaInfo metaInfoLeft,
                                  MetaInfo metaInfoRight,
                                  Row row)
            throws InvalidOperandTypeException,
            DivideZeroException,
            InvalidOperatorException,
            InvalidStatementException,
            DuplicateMatchedException,
            TableNotExistException,
            ColumnDoesNotExistException,
            OperandTypeNotMatchedException
    {
        String op = condition.op;
        Comparable resultLeft = calcExpression(
                condition.expressionLeft,
                metaInfoLeft,
                metaInfoRight,
                row);
        Comparable resultRight = calcExpression(
                condition.expressionRight,
                metaInfoLeft,
                metaInfoRight,
                row);
        if ((resultLeft instanceof String) ^ (resultRight instanceof String)) {
            throw new OperandTypeNotMatchedException();
        }
        int compareResult = resultLeft.toString().compareTo(resultRight.toString());
        switch (op) {
            case "=": return compareResult == 0;
            case "<>": return compareResult != 0;
            case "<": return compareResult < 0;
            case ">": return compareResult > 0;
            case "<=": return compareResult <= 0;
            case ">=": return compareResult >= 0;
            default: return false;
        }
    }

    private Comparable calcExpression(Expression expression,
                                      MetaInfo metaInfoLeft,
                                      MetaInfo metaInfoRight,
                                      Row row)
            throws InvalidOperandTypeException,
            DivideZeroException,
            InvalidOperatorException,
            InvalidStatementException,
            DuplicateMatchedException,
            TableNotExistException,
            ColumnDoesNotExistException
    {
        Comparer comparerLeft = expression.comparerLeft;
        Comparer comparerRight = expression.comparerRight;
        Expression.OP op = expression.op;
        if (comparerLeft != null && comparerRight == null && op == null) {
            return getValueFromComparer(comparerLeft,
                    metaInfoLeft,
                    metaInfoRight,
                    row
            );
        }
        else if (comparerLeft != null && comparerRight != null && op != null){
            Comparable comparableLeft = getValueFromComparer(
                    comparerLeft,
                    metaInfoLeft,
                    metaInfoRight,
                    row
            );
            Comparable comparableRight = getValueFromComparer(
                    comparerRight,
                    metaInfoLeft,
                    metaInfoRight,
                    row
            );
            if (comparableLeft instanceof String || comparableRight instanceof String) {
                throw new InvalidOperandTypeException();
            }
            Double doubleLeft = Double.valueOf(comparableLeft.toString());
            Double doubleRight = Double.valueOf(comparableRight.toString());
            switch (op) {
                case ADD: return doubleLeft + doubleRight;
                case SUB: return doubleLeft - doubleRight;
                case MUL: return doubleLeft * doubleRight;
                case DIV: {
                    if (doubleRight == 0) {
                        throw new DivideZeroException();
                    }
                    return doubleLeft / doubleRight;
                }
                default: throw new InvalidOperatorException();
            }
        }
        else {
            throw new InvalidStatementException();
        }
    }

    private Comparable getValueFromComparer(Comparer comparer,
                                            MetaInfo metaInfoLeft,
                                            MetaInfo metaInfoRight,
                                            Row row)
            throws DuplicateMatchedException,
            TableNotExistException,
            ColumnDoesNotExistException
    {
        if (comparer.get_type().equals(Comparer.Type.COLUMN_FULL_NAME)) {
            ColumnFullName fullName = (ColumnFullName)comparer;
            int index = getColumnIndex(fullName, metaInfoLeft, metaInfoRight);
            if (index == -1) {
                throw new ColumnDoesNotExistException();
            }
            return row.getEntries().get(index).value;
        }
        else {
            return ((LiteralValue)comparer).value;
        }
    }
}