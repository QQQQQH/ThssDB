package cn.edu.thssdb.exception;

public class TableAlreadyExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: table already exists!";
    }
}
