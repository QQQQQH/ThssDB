package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.*;
import cn.edu.thssdb.parser.Statement.*;
import cn.edu.thssdb.server.ThssDB;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
    private HashMap<String, Database> databases;
    private String currentDatabase;
    private static SQLExecutor sqlExecutor;
    private static ReentrantReadWriteLock lock;

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    private Manager() {
        // TODO
        this.databases = new HashMap<>();
        this.currentDatabase = null;
        lock = new ReentrantReadWriteLock();
        sqlExecutor = new SQLExecutor();
    }

    private void createDatabaseIfNotExists(String name) throws DatabaseAlreadyExistException {
        // TODO
        try {
            lock.writeLock().lock();
            if (databases.get(name) != null) {
                throw new DatabaseAlreadyExistException();
            }
            Database database = new Database(name);
            databases.put(name, database);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void deleteDatabase(String name) throws DatabaseNotExistException {
        // TODO
        try {
            lock.writeLock().lock();
            if (databases.get(name) == null) {
                throw new DatabaseNotExistException();
            }

            if (currentDatabase != null && currentDatabase.equals(name)) {
                currentDatabase = null;
            }

            databases.remove(name);
            // remove database file
            Path databaseDirector = Paths.get(name);
            Files.walkFileTree(databaseDirector, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }

            });
        } catch (IOException e) {
            System.err.println("Fail to remove database file!");
            e.printStackTrace();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void switchDatabase(String name) throws DatabaseNotExistException {
        // TODO
        try {
            lock.writeLock().lock();
            if (databases.get(name) == null) {
                throw new DatabaseNotExistException();
            }
            currentDatabase = name;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Database getDatabase() throws DatabaseNotSelectException {
        try {
            lock.writeLock().lock();
            if (currentDatabase == null) {
                throw new DatabaseNotSelectException();
            }
            else {
                return databases.get(currentDatabase);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<SQLExecutor.SQLExecuteResult> execute(String sql) {
        List<Statement> statementList = sqlExecutor.parseSQL(sql);
        if (statementList == null || statementList.size() == 0) {
            List<SQLExecutor.SQLExecuteResult> resultList = new ArrayList<>();
            resultList.add(new SQLExecutor.SQLExecuteResult("SQL syntax error! Check your statement.", false, false));
            return resultList;
        }
        else {
            return sqlExecutor.executeSQL(statementList);
        }
    }

    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager();

        private ManagerHolder() {

        }
    }

    public static class SQLExecutor {
        private SQLExecutor() {}

        public static class SQLExecuteResult {
            private String msg;
            private List<String> columnList;
            private List<List<String>> rowList;
            private boolean isSucceed;
            private boolean hasResult;
            private boolean isAbort;

            SQLExecuteResult(String msg, boolean isSucceed, boolean isAbort) {
                this.msg = msg;
                this.isSucceed = isSucceed;
                this.isAbort = isAbort;
                this.hasResult = false;
                this.columnList = null;
                this.rowList = null;
            }

            SQLExecuteResult(String msg,
                             List<String> columnList,
                             List<List<String>> rowList) {
                this.msg = msg;
                this.isSucceed = true;
                this.isAbort = false;
                this.hasResult = true;
                this.columnList = columnList;
                this.rowList = rowList;
            }

            public String getMessage() { return msg; }
            public boolean isIsSucceed() { return isSucceed; }
            public boolean isHasResult() { return hasResult; }
            public boolean isIsAbort() { return isAbort; }
            public List<String> getColumnList() { return columnList; }
            public List<List<String>> getRowList() { return rowList; }
        }

        @SuppressWarnings("unchecked")
        private ArrayList<Statement> parseSQL(String sql) {
            try {
                CharStream input = CharStreams.fromString(sql);
                SQLLexer lexer = new SQLLexer(input);
                CommonTokenStream tokens = new CommonTokenStream(lexer);
                SQLParser parser = new SQLParser(tokens);
                ParseTree tree = parser.parse();
                MySQLVisitor visitor = new MySQLVisitor();
                return (ArrayList<Statement>) visitor.visit(tree);
            }
            catch (Exception e) {
                return null;
            }

        }

        private ArrayList<SQLExecuteResult> executeSQL(List<Statement> statementList) {
            ArrayList<SQLExecuteResult> resultList = new ArrayList<>();
            for (Statement statement: statementList) {
                System.out.println(statement.get_type());
                switch (statement.get_type()) {
                    case CREATE_DATABASE:
                        resultList.add(createDatabase((CreatDatabaseStatement)statement));
                        break;
                    case DROP_DATABASE:
                        resultList.add(dropDatabase((DropDatabaseStatement)statement));
                        break;
                    case USE:
                        resultList.add(useDatabase((UseDatabaseStatement)statement));
                        break;
                    case CREATE_TABLE:
                        resultList.add(createTable((CreateTableStatement)statement));
                        break;
                    default:
                        resultList.add(new SQLExecuteResult("SQL syntax supported!", false, false));
                        break;
                }
            }
            return resultList;
        }

        private SQLExecuteResult createDatabase(CreatDatabaseStatement statement) {
            try {
                Manager.getInstance().createDatabaseIfNotExists(statement.databaseName);
                return new SQLExecuteResult("Create database succeeds.", true, false);
            }
            catch (DatabaseAlreadyExistException e) {
                return new SQLExecuteResult("Database already exists!", false, false);
            }
        }

        private SQLExecuteResult dropDatabase(DropDatabaseStatement statement) {
            try {
                Manager.getInstance().deleteDatabase(statement.databaseName);
                return new SQLExecuteResult("Drop database succeeds.", true, false);
            }
            catch (DatabaseNotExistException e) {
                return new SQLExecuteResult("Database doesn't exist!", false, false);
            }
        }

        private SQLExecuteResult useDatabase(UseDatabaseStatement statement) {
            try {
                Manager.getInstance().switchDatabase(statement.databaseName);
                return new SQLExecuteResult("Database switch to "+statement.databaseName+".", true, false);
            }
            catch(DatabaseNotExistException e) {
                return new SQLExecuteResult("Database doesn't exist!", false, false);
            }
        }

        private SQLExecuteResult createTable(CreateTableStatement statement) {
            try {
                Database database = Manager.getInstance().getDatabase();
                database.create(statement.tableName, statement.columnDefList, statement.primaryKey);
                return new SQLExecuteResult("Create table succeeds.", true, false);
            }
            catch (DatabaseNotSelectException e) {
                return new SQLExecuteResult("Database hasn't been selected!", false, false);
            }
            catch (ColumnDoesNotExistException e) {
                return new SQLExecuteResult("Primary key doesn't exist in columns!", false, false);
            }
            catch (TableAlreadyExistException e) {
                return new SQLExecuteResult("Table already exists!", false, false);
            }
        }

    }
}
