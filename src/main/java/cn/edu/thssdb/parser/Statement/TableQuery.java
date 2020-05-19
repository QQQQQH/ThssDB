package cn.edu.thssdb.parser.Statement;

public class TableQuery {
    public Condition condition = null;
    public String tableNameLeft, tableNameRight = null;

    public TableQuery(String tableNameLeft) {
        this.tableNameLeft = tableNameLeft;
    }

    public TableQuery(String tableNameLeft, String tableNameRight, Condition condition) {
        this.tableNameLeft = tableNameLeft;
        this.tableNameRight = tableNameRight;
        this.condition = condition;
    }
}
