package cn.edu.thssdb.exception;

public class DuplicateMatchedException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Multiple tables or columns are matched!";
    }
}
