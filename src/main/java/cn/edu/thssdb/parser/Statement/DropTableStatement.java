package cn.edu.thssdb.parser.Statement;

public class DropTableStatement extends Statement {
    public String tableName;

    @Override
    public Type get_type() {
        return Type.DROP_TABLE;
    }

    public DropTableStatement(String tableName) {
        this.tableName = tableName;
    }

}
