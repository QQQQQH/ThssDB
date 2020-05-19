package cn.edu.thssdb.exception;

public class ColumnDoesNotExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: Column doesn't exist!";
    }
}
