package cn.edu.thssdb.parser;

import java.util.ArrayList;
import java.util.List;

public class MySQLVisitor extends SQLBaseVisitor<Object> {
    /*
        parse :
            sql_stmt_list ;
     */

    @Override
    public Object visitParse(SQLParser.ParseContext ctx) {
        return visit(ctx.sql_stmt_list());
    }


    /*
        sql_stmt_list :
            ';'* sql_stmt ( ';'+ sql_stmt )* ';'* ;
     */

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
        sql_stmt :
            create_db_stmt
            | drop_db_stmt
            | create_table_stmt
            | drop_table_stmt
            | show_table_stmt
            | insert_stmt
            | delete_stmt
            | update_stmt
            | select_stmt ;

     */

    @Override
    public Object visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        return visit(ctx.getChild(0));
    }


    /*
        create_db_stmt :
            K_CREATE K_DATABASE database_name ;
     */
    @Override
    public Object visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        String databaseName = (String) visit(ctx.database_name());
        return new CreatDatabaseStatement(databaseName);
    }


    /*
        Drop Database
        K_DROP K_DATABASE(K_IF K_EXISTS)?database_name;
     */
    @Override
    public Object visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        String databaseName = (String) visit(ctx.database_name());
        return new DropDatabaseStatement(databaseName);
    }


    /*
        Create Table
        K_CREATE K_TABLE table_name
            '(' column_def ( ',' column_def )* ( ',' table_constraint )? ')' ;
     */
    @Override
    public Object visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        String primaryKey = null;
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


    /*
        Insert
        K_INSERT K_INTO table_name ( '(' column_name ( ',' column_name )* ')' )?
            K_VALUES value_entry ( ',' value_entry )* ;
     */
    @Override
    public Object visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        ArrayList<String> columnNameList = new ArrayList<>();
        List<SQLParser.Column_nameContext> nameContextList = ctx.column_name();
        for (SQLParser.Column_nameContext nameContext : nameContextList) {
            columnNameList.add((String) visit(nameContext));
        }

        ArrayList<Attribute> valueList = new ArrayList<>();
        List<SQLParser.Value_entryContext> entryContextList = ctx.value_entry();
        for (SQLParser.Value_entryContext entryContext : entryContextList) {
            valueList.add((Attribute) visit(entryContext));
        }
        return new InsertStatement(tableName, columnNameList, valueList);
    }


    /*
        Delete From
        K_DELETE K_FROM table_name ( K_WHERE multiple_condition )? ;
     */
    @Override
    public Object visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableNmae = ctx.table_name().getText();
        Condition condition = (Condition) visit(ctx.multiple_condition());
        return new DeleteStatement(tableNmae, condition);
    }


    /*
        Update
        K_UPDATE table_name
            K_SET column_name '=' expression ( K_WHERE multiple_condition )? ;
     */
    @Override
    public Object visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        String tableName = ctx.table_name().getText(),
                columnName = ctx.column_name().getText();
        Comparable experssion = (Comparable) visit(ctx.expression());
        return new UpdateStatement(tableName, columnName, experssion);
    }


    /*
        Select
        K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
        K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Override
    public Object visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        ArrayList<String> resultColumnNameList = new ArrayList<>();
        List<SQLParser.Result_columnContext> columnContextList = ctx.result_column();
        for (SQLParser.Result_columnContext columnContext : columnContextList) {
            resultColumnNameList.add((String) visit(columnContext));
        }

        TableQuery tableQuery = (TableQuery) visit(ctx.table_query(0));
        Condition condition = null;
        if (ctx.multiple_condition() != null) {
            condition = (Condition) visit(ctx.multiple_condition());
        }
        return new SelectStatement(resultColumnNameList, tableQuery, condition);
    }


    /*
        database_name :
            IDENTIFIER ;
     */
    @Override
    public Object visitDatabase_name(SQLParser.Database_nameContext ctx) {
        return ctx.IDENTIFIER().getText();
    }


    /*
        table_query:
        table_name
        | table_name ( K_JOIN table_name )+ K_ON multiple_condition ;
     */
    @Override
    public Object visitTable_query(SQLParser.Table_queryContext ctx) {
        if (ctx.table_name(1) == null) {
            return new TableQuery(1, ctx.table_name(0).getText());
        } else {
            return new TableQuery(2,
                    ctx.table_name(0).getText(),
                    ctx.table_name(1).getText(),
                    (Condition) visit(ctx.multiple_condition()));
        }
    }
}


