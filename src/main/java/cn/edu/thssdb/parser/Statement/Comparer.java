package cn.edu.thssdb.parser.Statement;

public abstract class Comparer {
    public enum Type {
        COLUMN_FULL_NAME, LITERAL_VALUE;
    }

    public abstract Type get_type();
}
