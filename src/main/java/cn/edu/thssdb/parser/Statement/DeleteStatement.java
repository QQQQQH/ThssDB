package cn.edu.thssdb.parser.Statement;

public class DeleteStatement extends Statement {
    public String tableName;
    public Condition condition;

    @Override
    public Type get_type() {
        return Type.DELETE;
    }

    public DeleteStatement(String tableName, Condition condition) {
        this.tableName = tableName;
        this.condition = condition;
    }
}
