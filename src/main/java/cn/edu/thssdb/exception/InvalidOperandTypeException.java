package cn.edu.thssdb.exception;

public class InvalidOperandTypeException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Invalid operand type, expects INT LONG FLOAT DOUBLE!";
    }
}
