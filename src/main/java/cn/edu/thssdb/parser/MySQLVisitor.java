package cn.edu.thssdb.parser;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.List;

public class MySQLVisitor extends SQLBaseVisitor<Object> {
    @Override
    public Object visitParse(SQLParser.ParseContext ctx) {
        return visit(ctx.sql_stmt_list());
    }

    @Override
    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<Statement> statementList = new ArrayList<>();
        List<SQLParser.Sql_stmtContext> stmtContextList = ctx.sql_stmt();
        for (SQLParser.Sql_stmtContext stmtContext : stmtContextList) {
            statementList.add((Statement) visit(stmtContext));
        }
        return statementList;
    }

    /*
        Create Database
        K_CREATE K_DATABASE database_name ;
     */
    @Override
    public Object visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        String databaseName = ctx.database_name().getText();
        return new CreatDatabaseStatement(databaseName);
    }


    /*
        Drop Database
        K_DROP K_DATABASE(K_IF K_EXISTS)?database_name;
     */
    @Override
    public Object visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        String databaseName = ctx.database_name().getText();
        return new DropDatabaseStatement(databaseName);
    }


    /*
        Create Table
        K_CREATE K_TABLE table_name
            '(' column_def ( ',' column_def )* ( ',' table_constraint )? ')' ;
     */
    @Override
    public Object visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText(), primaryKey = null;
        ArrayList<ColumnDef> columnDefList = new ArrayList<>();
        List<SQLParser.Column_defContext> defContextList = ctx.column_def();
        for (SQLParser.Column_defContext defContext : defContextList) {
            columnDefList.add((ColumnDef) visit(defContext));
        }
        if (ctx.table_constraint() != null) {
            primaryKey = (String) visit(ctx.table_constraint());
        }
        return new CreateTableStatement(tableName, columnDefList, primaryKey);
    }


    /*
        Drop Table
        K_DROP K_TABLE ( K_IF K_EXISTS )? table_name ;
     */
    @Override
    public Object visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        return new DropTableStatement(tableName);
    }


    /*
        Show Table
        K_SHOW K_TABLE table_name ;
     */
    @Override
    public Object visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        return new ShowMetaStatement(tableName);
    }


}


