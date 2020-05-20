package cn.edu.thssdb.exception;

public class ColumnDoesNotExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Column doesn't exist!";
    }
}
