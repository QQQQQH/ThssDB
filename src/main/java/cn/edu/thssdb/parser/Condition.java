package cn.edu.thssdb.parser;

public class Condition {
    public enum Type {
        COLUMN_NAME, LITERAL
    }

    public Type type;
    public String op;
    public Comparable left, right;

    public Condition(Type type, String op, Comparable left, Comparable right) {
        this.type = type;
        this.op = op;
        this.left = left;
        this.right = right;
    }
}
