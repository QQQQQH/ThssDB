package cn.edu.thssdb.exception;

public class ColumnValueSizeNotMatchedException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Column and value list size not matched!";
    }
}
