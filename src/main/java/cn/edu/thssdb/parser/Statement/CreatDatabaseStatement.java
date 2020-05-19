package cn.edu.thssdb.parser.Statement;

public class CreatDatabaseStatement extends Statement {
    public String databaseName;

    @Override
    public Type get_type() {
        return Type.CREATE_DATABASE;
    }

    public CreatDatabaseStatement(String databaseName) {
        this.databaseName = databaseName;
    }

}
