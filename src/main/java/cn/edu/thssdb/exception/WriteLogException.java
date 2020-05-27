package cn.edu.thssdb.exception;

public class WriteLogException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: fail to write log!";
    }
}
