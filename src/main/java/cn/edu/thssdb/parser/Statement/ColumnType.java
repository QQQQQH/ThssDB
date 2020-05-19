package cn.edu.thssdb.parser.Statement;

public class ColumnType {
    public enum Type {
        INT, LONG, FLOAT, DOUBLE, STRING
    }

    public Type type;
    public int num = 0;

    public ColumnType(Type type) {
        this.type = type;
    }

    public ColumnType(Type type, int num) {
        this.type = type;
        this.num = num;
    }
}
