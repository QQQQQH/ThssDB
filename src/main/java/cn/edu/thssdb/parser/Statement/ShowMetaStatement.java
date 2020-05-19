package cn.edu.thssdb.parser.Statement;

public class ShowMetaStatement extends Statement {
    public String tableName;

    @Override
    public Type get_type() {
        return Type.SHOW_META;
    }

    public ShowMetaStatement(String tableName) {
        this.tableName = tableName;
    }
}
