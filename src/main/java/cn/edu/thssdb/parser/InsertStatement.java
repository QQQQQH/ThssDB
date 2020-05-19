package cn.edu.thssdb.parser;

import java.util.ArrayList;

public class InsertStatement extends Statement {
    public String tableName;
    public ArrayList<String> columnNameList;
    public ArrayList<Attribute> valueList;

    @Override
    public Type get_type() {
        return Type.INSERT;
    }

    public InsertStatement(String tableName, ArrayList<String> columnNameList, ArrayList<Attribute> valueList) {
        this.tableName = tableName;
        this.columnNameList = columnNameList;
        this.valueList = valueList;
    }
}
