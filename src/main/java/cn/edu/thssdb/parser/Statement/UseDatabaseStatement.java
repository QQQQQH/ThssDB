package cn.edu.thssdb.parser.Statement;

public class UseDatabaseStatement extends Statement {
    public String databaseName;

    @Override
    public Type get_type() {
        return Type.USE;
    }

    public UseDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

}
