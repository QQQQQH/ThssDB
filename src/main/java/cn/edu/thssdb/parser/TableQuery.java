package cn.edu.thssdb.parser;

public class TableQuery {
    public int tableCnt;
    public Condition condition;
    public String tableNameLeft, tableNameRight;

    public TableQuery(int tableCnt, String tableNameLeft) {
        this.tableCnt = tableCnt;
        this.tableNameLeft = tableNameLeft;
    }

    public TableQuery(int tableCnt, String tableNameLeft, String tableNameRight, Condition condition) {
        this.tableCnt = tableCnt;
        this.tableNameLeft = tableNameLeft;
        this.tableNameRight = tableNameRight;
        this.condition = condition;
    }
}
