package cn.edu.thssdb.parser.Statement;

public class ColumnDef {
    public String columnName;
    public ColumnType columnType;
    public boolean notNull;

    public ColumnDef(String columnName, ColumnType columnType, boolean notNull) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.notNull = notNull;
    }
}
