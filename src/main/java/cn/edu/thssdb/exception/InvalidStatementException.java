package cn.edu.thssdb.exception;

public class InvalidStatementException extends RuntimeException {
    @Override
    public String getMessage() {
        return "SQL syntax error! Check your statement.";
    }
}
