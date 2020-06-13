package cn.edu.thssdb.exception;

public class TableAlreadyExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Table already exists!";
    }
}
