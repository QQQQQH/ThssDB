package cn.edu.thssdb.exception;

public class InvalidOperatorException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Invalid operand type, expects + - * / !";
    }
}
