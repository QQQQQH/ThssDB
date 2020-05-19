package cn.edu.thssdb.parser.Statement;

public class DropDatabaseStatement extends Statement {
    public String databaseName;

    @Override
    public Type get_type() {
        return Type.DROP_DATABASE;
    }

    public DropDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

}
