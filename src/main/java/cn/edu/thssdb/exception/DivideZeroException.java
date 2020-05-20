package cn.edu.thssdb.exception;

public class DivideZeroException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Divide zero detected!";
    }
}
