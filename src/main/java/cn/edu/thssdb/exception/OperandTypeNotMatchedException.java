package cn.edu.thssdb.exception;

public class OperandTypeNotMatchedException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Operand types aren't matched!";
    }
}
