package cn.edu.thssdb.parser;

public class ColumnDef {
    public enum Type {
        INT, LONG, FLOAT, DOUBLE, STRING
    }

    public Type type;
    public int num = 0;
    public boolean notNull;

    public ColumnDef(Type type, boolean notNull) {
        this.type = type;
        this.notNull = notNull;
    }

    public ColumnDef(Type type, boolean notNull, int num) {
        this.type = type;
        this.notNull = notNull;
        this.num = num;
    }
}
