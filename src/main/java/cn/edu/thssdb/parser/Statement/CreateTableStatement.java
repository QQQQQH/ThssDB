package cn.edu.thssdb.parser.Statement;

import java.util.ArrayList;

public class CreateTableStatement extends Statement {
    public String tableName, primaryKey;
    public ArrayList<ColumnDef> columnDefList;

    @Override
    public Type get_type() {
        return Type.CREATE_TABLE;
    }

    public CreateTableStatement(String TableName, ArrayList<ColumnDef> columnDefList, String primaryKey) {
        this.tableName = TableName;
        this.columnDefList = columnDefList;
        this.primaryKey = primaryKey;
    }

}
