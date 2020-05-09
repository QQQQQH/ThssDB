package cn.edu.thssdb.exception;

public class DatabaseAlreadyExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Exception: database already exists!";
    }
}
