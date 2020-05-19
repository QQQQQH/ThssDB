package cn.edu.thssdb.parser;

import java.util.ArrayList;

public class SelectStatement extends Statement {
    public ArrayList<String> resultColumnNameList;
    public TableQuery tableQuery;
    public Condition condition;

    @Override
    public Type get_type() {
        return Type.SELECT;
    }

    public SelectStatement(ArrayList<String> resultColumnNameList, TableQuery tableQuery, Condition condition) {
        this.resultColumnNameList = resultColumnNameList;
        this.tableQuery = tableQuery;
        this.condition = condition;
    }
}
