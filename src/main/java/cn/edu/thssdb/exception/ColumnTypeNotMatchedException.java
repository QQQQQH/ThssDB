package cn.edu.thssdb.exception;

public class ColumnTypeNotMatchedException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Column type not matched!";
    }
}
