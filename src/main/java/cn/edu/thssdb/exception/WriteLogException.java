package cn.edu.thssdb.exception;

public class WriteLogException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Fail to write log!";
    }
}
