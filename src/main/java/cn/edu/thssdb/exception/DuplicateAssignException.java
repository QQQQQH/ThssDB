package cn.edu.thssdb.exception;

public class DuplicateAssignException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Duplicate assignment detected!";
    }
}
