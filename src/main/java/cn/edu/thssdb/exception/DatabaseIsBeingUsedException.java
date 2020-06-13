package cn.edu.thssdb.exception;

public class DatabaseIsBeingUsedException extends RuntimeException {
    @Override
    public String getMessage() {
        return "Error: Can't delete database for it's being used!";
    }
}