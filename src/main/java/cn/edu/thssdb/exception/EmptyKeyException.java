package cn.edu.thssdb.exception;

public class EmptyKeyException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Primary key or not null key is empty!";
    }
}
