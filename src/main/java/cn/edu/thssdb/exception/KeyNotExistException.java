package cn.edu.thssdb.exception;

public class KeyNotExistException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: key doesn't exist!";
    }
}
