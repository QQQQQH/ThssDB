package cn.edu.thssdb.parser;

import cn.edu.thssdb.parser.Statement.*;

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
        drop_table_stmt :
            K_DROP K_TABLE ( K_IF K_EXISTS )? table_name ;
     */
    @Override
    public Object visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        String databaseName = (String) visit(ctx.database_name());
        return new DropDatabaseStatement(databaseName);
    }

    /*
        use_db_stmt :
            K_USE database_name;
     */
    @Override
    public Object visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        String databaseName = (String) visit(ctx.database_name());
        return new UseDatabaseStatement(databaseName);
    }


    /*
        create_table_stmt :
            K_CREATE K_TABLE table_name
            '(' column_def ( ',' column_def )* ( ',' table_constraint )? ')' ;
     */
    @Override
    public Object visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tableName = (String) visit(ctx.table_name());
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
        drop_table_stmt :
            K_DROP K_TABLE ( K_IF K_EXISTS )? table_name ;
     */
    @Override
    public Object visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = (String) visit(ctx.table_name());
        return new DropTableStatement(tableName);
    }


    /*
        show_table_stmt :
            K_SHOW K_DATABASE database_name;
     */
    @Override
    public Object visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        String tableName = (String) visit(ctx.table_name());
        return new ShowMetaStatement(tableName);
    }


    /*
        insert_stmt :
        K_INSERT K_INTO table_name ( '(' column_name ( ',' column_name )* ')' )?
            K_VALUES value_entry ( ',' value_entry )* ;
     */
    @Override
    public Object visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = (String) visit(ctx.table_name());

        ArrayList<String> columnNameList = new ArrayList<>();
        if (ctx.column_name() != null) {
            List<SQLParser.Column_nameContext> nameContextList = ctx.column_name();
            for (SQLParser.Column_nameContext nameContext : nameContextList) {
                columnNameList.add((String) visit(nameContext));
            }
        }

        ArrayList<Comparable> valueList = (ArrayList<Comparable>) visit(ctx.value_entry(0));
        return new InsertStatement(tableName, columnNameList, valueList);
    }


    /*
        delete_stmt :
        K_DELETE K_FROM table_name ( K_WHERE multiple_condition )? ;
     */
    @Override
    public Object visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableName = (String) visit(ctx.table_name());

        Condition condition = null;
        if (ctx.multiple_condition() != null) {
            condition = (Condition) visit(ctx.multiple_condition());
        }
        return new DeleteStatement(tableName, condition);
    }


    /*
        update_stmt :
        K_UPDATE table_name
            K_SET column_name '=' expression ( K_WHERE multiple_condition )? ;
     */
    @Override
    public Object visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        String tableName = (String) visit(ctx.table_name()),
                columnName = (String) visit(ctx.column_name());
        Expression experssion = (Expression) visit(ctx.expression());
        Condition condition = null;
        if (ctx.multiple_condition() != null) {
            condition = (Condition) visit(ctx.multiple_condition());
        }
        return new UpdateStatement(tableName, columnName, experssion, condition);
    }


    /*
        select_stmt :
            K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
                K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Override
    public Object visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        ArrayList<ColumnFullName> resultColumnNameList = new ArrayList<>();
        List<SQLParser.Result_columnContext> columnContextList = ctx.result_column();
        for (SQLParser.Result_columnContext columnContext : columnContextList) {
            resultColumnNameList.add((ColumnFullName) visit(columnContext));
        }

        TableQuery tableQuery = (TableQuery) visit(ctx.table_query(0));
        Condition condition = null;
        if (ctx.multiple_condition() != null) {
            condition = (Condition) visit(ctx.multiple_condition());
        }
        return new SelectStatement(resultColumnNameList, tableQuery, condition);
    }


    /*
        comparator :
            EQ | NE | LE | GE | LT | GT ;
     */
    @Override
    public Object visitComparator(SQLParser.ComparatorContext ctx) {
        return ctx.getText();
    }


    /*
        condition :
            expression comparator expression;
     */
    @Override
    public Object visitCondition(SQLParser.ConditionContext ctx) {
        return new Condition((Expression) visit(ctx.expression(0)),
                (String) visit(ctx.comparator()),
                (Expression) visit(ctx.expression(1)));
    }


    /*
        column_constraint :
            K_PRIMARY K_KEY
            | K_NOT K_NULL ;
     */
    @Override
    public Object visitColumn_constraint(SQLParser.Column_constraintContext ctx) {
        return ctx.K_NOT() != null && ctx.K_NULL() != null;
    }


    /*
        column_def :
            column_name type_name column_constraint* ;
     */
    @Override
    public Object visitColumn_def(SQLParser.Column_defContext ctx) {
        String columnName = (String) visit(ctx.column_name());
        ColumnType columnType = (ColumnType) visit(ctx.type_name());
        boolean notNull = false;
        if (ctx.column_constraint(0) != null) {
            notNull = (boolean) visit(ctx.column_constraint(0));
        }
        return new ColumnDef(columnName, columnType, notNull);
    }


    /*
        column_full_name:
            ( table_name '.' )? column_name ;
     */

    @Override
    public Object visitColumn_full_name(SQLParser.Column_full_nameContext ctx) {
        String tableName = null;
        if (ctx.table_name() != null) {
            tableName = (String) visit(ctx.table_name());
        }
        String columnName = (String) visit(ctx.column_name());
        return new ColumnFullName(tableName, columnName);
    }


    /*
        column_name :
            IDENTIFIER ;
     */
    @Override
    public Object visitColumn_name(SQLParser.Column_nameContext ctx) {
        return ctx.IDENTIFIER().getText();
    }


    /*
        comparer :
            column_full_name
            | literal_value ;
     */
    @Override
    public Object visitComparer(SQLParser.ComparerContext ctx) {
        Comparer comparer;
        if (ctx.column_full_name() != null) {
            comparer = (ColumnFullName) visit(ctx.column_full_name());
        } else {
            comparer = new LiteralValue((Comparable) visit(ctx.literal_value()));
        }
        return comparer;
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
        expression :
            comparer
            | expression ( MUL | DIV ) expression
            | expression ( ADD | SUB ) expression
            | '(' expression ')';
     */
    @Override
    public Object visitExpression(SQLParser.ExpressionContext ctx) {
        Expression expression;
        if (ctx.comparer() != null) {
            expression = new Expression((Comparer) visit(ctx.comparer()));
        } else if (ctx.expression(1) != null) {
            Comparer comparerLeft = ((Expression) visit(ctx.expression(0))).comparerLeft;
            Comparer comparerRight = ((Expression) visit(ctx.expression(1))).comparerLeft;
            if (ctx.MUL() != null) {
                expression = new Expression(comparerLeft, Expression.OP.MUL, comparerRight);
            } else if (ctx.DIV() != null) {
                expression = new Expression(comparerLeft, Expression.OP.DIV, comparerRight);
            } else if (ctx.ADD() != null) {
                expression = new Expression(comparerLeft, Expression.OP.ADD, comparerRight);
            } else {
                expression = new Expression(comparerLeft, Expression.OP.SUB, comparerRight);
            }
        } else {
            expression = (Expression) visit(ctx.expression((0)));
        }
        return expression;
    }


    /*
        literal_value :
            NUMERIC_LITERAL
            | STRING_LITERAL
            | K_NULL ;
     */
    @Override
    public Object visitLiteral_value(SQLParser.Literal_valueContext ctx) {
        Comparable value = null;
        if (ctx.NUMERIC_LITERAL() != null) {
            String string = ctx.getText();
            if (string.contains(".") || string.contains("e")) {
                value = Double.valueOf(string);
            } else {
                value = Long.valueOf(string);
            }
        } else if (ctx.STRING_LITERAL() != null) {
            value = ctx.getText();
        }
        return value;
    }


    /*
        multiple_condition :
            condition
            | multiple_condition AND multiple_condition
            | multiple_condition OR multiple_condition ;
     */
    @Override
    public Object visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        return (Condition) visit(ctx.condition());
    }


    /*
        result_column
            : '*'
            | table_name '.' '*'
            | column_full_name;
     */
    @Override
    public Object visitResult_column(SQLParser.Result_columnContext ctx) {
        // null="*"
        ColumnFullName columnFullName;
        if (ctx.getText().equals("*")) {
            columnFullName = new ColumnFullName(null, null);
        } else if (ctx.table_name() != null) {
            String tableName = (String) visit(ctx.table_name());
            columnFullName = new ColumnFullName(tableName, null);
        } else {
            columnFullName = (ColumnFullName) visit(ctx.column_full_name());
        }
        return columnFullName;
    }


    /*
        table_constraint :
            K_PRIMARY K_KEY '(' column_name (',' column_name)* ')' ;
     */
    @Override
    public Object visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        return visit(ctx.column_name(0));
    }


    /*
        table_name :
            IDENTIFIER ;
     */
    @Override
    public Object visitTable_name(SQLParser.Table_nameContext ctx) {
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
//            return new TableQuery(ctx.table_name(0).getText());
            return new TableQuery((String)visit(ctx.table_name(0)));
        } else {
//            return new TableQuery(
//                    ctx.table_name(0).getText(),
//                    ctx.table_name(1).getText(),
//                    (Condition) visit(ctx.multiple_condition()));
            return new TableQuery(
                    (String)visit(ctx.table_name(0)),
                    (String)visit(ctx.table_name(1)),
                    (Condition) visit(ctx.multiple_condition()));
        }
    }


    /*
        type_name :
            T_INT
            | T_LONG
            | T_FLOAT
            | T_DOUBLE
            | T_STRING '(' NUMERIC_LITERAL ')' ;
     */
    @Override
    public Object visitType_name(SQLParser.Type_nameContext ctx) {
        if (ctx.T_INT() != null) {
            return new ColumnType(ColumnType.Type.INT);
        } else if (ctx.T_LONG() != null) {
            return new ColumnType(ColumnType.Type.LONG);
        } else if (ctx.T_FLOAT() != null) {
            return new ColumnType(ColumnType.Type.FLOAT);
        } else if (ctx.T_DOUBLE() != null) {
            return new ColumnType(ColumnType.Type.DOUBLE);
        } else {
            int num = Integer.parseInt(ctx.NUMERIC_LITERAL().getText());
            return new ColumnType(ColumnType.Type.STRING, num);
        }
    }


    /*
        value_entry :
            '(' literal_value ( ',' literal_value )* ')' ;
     */
    @Override
    public Object visitValue_entry(SQLParser.Value_entryContext ctx) {
        ArrayList<Comparable> valueList = new ArrayList<>();
        List<SQLParser.Literal_valueContext> valueContextList = ctx.literal_value();
        for (SQLParser.Literal_valueContext valueContext : valueContextList) {
            valueList.add((Comparable) visit(valueContext));
        }
        return valueList;
    }
}


