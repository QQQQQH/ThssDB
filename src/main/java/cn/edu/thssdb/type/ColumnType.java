package cn.edu.thssdb.type;

public enum ColumnType {
    INT, LONG, FLOAT, DOUBLE, STRING;
    public static boolean typeCheck(ColumnType type, Comparable elem) {
        switch (type) {
            case INT:
            case LONG:
                return elem instanceof Long;
            case FLOAT:
            case DOUBLE:
                return elem instanceof Double;
            case STRING:
                return elem instanceof String;
            default: return false;
        }
    }
}
