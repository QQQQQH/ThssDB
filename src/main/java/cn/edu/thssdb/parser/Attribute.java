package cn.edu.thssdb.parser;

public class Attribute {
    public enum Type {
        INT, LONG, FLOAT, DOUBLE, STRING
    }

    public ColumnDef.Type type;
    public Comparable value;

    public Attribute(Comparable value) {
        this.value = value;
    }
}
