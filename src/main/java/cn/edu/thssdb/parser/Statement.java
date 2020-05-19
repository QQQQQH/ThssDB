package cn.edu.thssdb.parser;

public abstract class Statement {
    protected enum Type {
        CREATE_DATABASE, DROP_DATABASE, CREATE_TABLE, DROP_TABLE, SHOW_META
    }

    public abstract Type get_type();
}
