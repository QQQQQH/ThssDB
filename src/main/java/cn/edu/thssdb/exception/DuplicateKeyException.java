package cn.edu.thssdb.exception;

public class DuplicateKeyException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Operation causes duplicated keys!";
    }
}
