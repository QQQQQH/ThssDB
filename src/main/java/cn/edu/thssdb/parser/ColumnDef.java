package cn.edu.thssdb.parser;

public class ColumnDef {
    public enum Type {
        INT, LONG, FLOAT, DOUBLE, STRING
    }

    public String columnName;
    public Type type;
    public int num = 0;
    public boolean notNull;

    public ColumnDef(String columnName, Type type, boolean notNull) {
        this.columnName = columnName;
        this.type = type;
        this.notNull = notNull;
    }

    public ColumnDef(String columnName, Type type, boolean notNull, int num) {
        this.columnName = columnName;
        this.type = type;
        this.notNull = notNull;
        this.num = num;
    }
}
