package cn.edu.thssdb.parser.Statement;

import java.util.ArrayList;

public class SelectStatement extends Statement {
    public ArrayList<ColumnFullName> resultColumnNameList;
    public TableQuery tableQuery;
    public Condition condition;

    @Override
    public Type get_type() {
        return Type.SELECT;
    }

    public SelectStatement(ArrayList<ColumnFullName> resultColumnNameList, TableQuery tableQuery, Condition condition) {
        this.resultColumnNameList = resultColumnNameList;
        this.tableQuery = tableQuery;
        this.condition = condition;
    }
}
