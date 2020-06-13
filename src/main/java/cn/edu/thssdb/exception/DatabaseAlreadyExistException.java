package cn.edu.thssdb.exception;

public class DatabaseAlreadyExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Database already exists!";
    }
}
