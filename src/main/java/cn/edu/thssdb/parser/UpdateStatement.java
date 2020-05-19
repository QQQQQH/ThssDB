package cn.edu.thssdb.parser;

public class UpdateStatement extends Statement {
    public String tableName, columnName;
    public Comparable expression;

    @Override
    public Type get_type() {
        return Type.UPDATE;
    }

    public UpdateStatement(String tableNamem, String columnName, Comparable expression) {
        this.tableName = tableNamem;
        this.columnName = columnName;
        this.expression = expression;
    }
}
