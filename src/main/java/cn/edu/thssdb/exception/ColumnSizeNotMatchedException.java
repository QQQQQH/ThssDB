package cn.edu.thssdb.exception;

public class ColumnSizeNotMatchedException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Column size not matched!";
    }
}
