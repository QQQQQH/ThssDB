package cn.edu.thssdb.exception;

public class DatabaseNotSelectException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: database not selected!";
    }
}
