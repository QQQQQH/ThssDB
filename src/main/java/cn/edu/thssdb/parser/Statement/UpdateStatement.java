package cn.edu.thssdb.parser.Statement;

public class UpdateStatement extends Statement {
    public String tableName, columnName;
    public Expression expression;
    public Condition condition;

    @Override
    public Type get_type() {
        return Type.UPDATE;
    }

    public UpdateStatement(String tableNamem, String columnName, Expression expression, Condition condition) {
        this.tableName = tableNamem;
        this.columnName = columnName;
        this.expression = expression;
        this.condition = condition;
    }
}
