package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.*;
import cn.edu.thssdb.parser.Statement.*;
import cn.edu.thssdb.query.QueryResult;

import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
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
        databases = new HashMap<>();
        currentDatabase = null;
        lock = new ReentrantReadWriteLock();
        sqlExecutor = new SQLExecutor();
        recover();
    }

    private void persist() {
        File dir = new File(Global.DATABASE_DIR);
        if (!dir.exists() && !dir.mkdirs()) {
            System.err.println("Fail to persist manager due to mkdirs error!");
            return;
        }
        try {
            lock.writeLock().lock();
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dir.toString()+File.separator+"DATABASES_NAME"));
            for (String databaseName: databases.keySet()) {
                oos.writeObject(databaseName);
            }
            if (currentDatabase != null) {
                Database database = databases.get(currentDatabase);
                if (database == null) {
                    System.err.println("Current database is null while trying to persist!");
                }
                else {
                    database.quit();
                }
            }
        }
        catch (FileNotFoundException e) {
            System.err.print("Fail to persist manager due to FileNotFoundException!");
        }
        catch (IOException e) {
            System.err.print("Fail to persist manager due to IOException!");
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void recover() {
        File file = new File(Global.DATABASE_DIR+File.separator+"DATABASES_NAME");
        if (!file.exists()) return;
        try {
            lock.writeLock().lock();
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            while (fis.available() > 0) {
                String databaseName = (String)ois.readObject();
                databases.put(databaseName, new Database(databaseName));
            }
        }
        catch (FileNotFoundException e) {
            System.err.println("Fail to recover manager due to FileNotFoundException!");
        }
        catch (IOException e) {
            System.err.println("Fail to recover manager due to IOException!");
        }
        catch (ClassNotFoundException e) {
            System.err.println("Fail to recover manager due to ClassNotFoundException!");
        }
        finally {
            lock.writeLock().unlock();
        }
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
            Path databaseDirector = Paths.get(Global.DATABASE_DIR+File.separator+name);
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
        if (name.equals(currentDatabase)) return;
        try {
            lock.writeLock().lock();
            Database database = databases.get(name);
            if (database == null) {
                throw new DatabaseNotExistException();
            }
            database.recover();
            if (currentDatabase != null) {
                Database current = databases.get(currentDatabase);
                if (current == null) {
                    System.err.println("Current database is null while trying to persist!");
                }
                else {
                    current.quit();
                }
            }
            currentDatabase = name;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void quit() {
        persist();
        currentDatabase = null;
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
                    case DROP_TABLE:
                        resultList.add(dropTable((DropTableStatement)statement));
                        break;
                    case INSERT:
                        resultList.add(insert((InsertStatement)statement));
                        break;
                    case SELECT:
                        resultList.add(select((SelectStatement)statement));
                        break;
                    case DELETE:
                        resultList.add(delete((DeleteStatement)statement));
                        break;
                    case UPDATE:
                        resultList.add(update((UpdateStatement)statement));
                        break;
                    default:
                        resultList.add(new SQLExecuteResult("Error: SQL syntax not supported!", false, false));
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
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult dropDatabase(DropDatabaseStatement statement) {
            try {
                Manager.getInstance().deleteDatabase(statement.databaseName);
                return new SQLExecuteResult("Drop database succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult useDatabase(UseDatabaseStatement statement) {
            try {
                Manager.getInstance().switchDatabase(statement.databaseName);
                return new SQLExecuteResult("Database switch to "+statement.databaseName+".", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult createTable(CreateTableStatement statement) {
            try {
                Database database = Manager.getInstance().getDatabase();
                if (database.checkTableExist(statement.tableName)) {
                    throw new TableAlreadyExistException();
                }
                ArrayList<Column> columnsList = new ArrayList<>();
                for (ColumnDef columnDef: statement.columnDefList) {
                    columnsList.add(new Column(columnDef.columnName, // name
                            ColumnType.valueOf(columnDef.columnType.type.toString()), // ColumnType
                            0, // primary
                            columnDef.notNull, // notNull
                            columnDef.columnType.num)); // maxLength
                }
                /*
                 * Set primary key.
                 * If primaryKey == null then set first column as primary key
                 * */
                if (statement.primaryKey == null) {
                    columnsList.get(0).setPrimary();
                }
                else {
                    boolean columnExist = false;
                    for (Column column : columnsList) {
                        if (column.getName().equals(statement.primaryKey)) {
                            columnExist = true;
                            column.setPrimary();
                            break;
                        }
                    }
                    if (!columnExist) {
                        throw new ColumnDoesNotExistException();
                    }
                }
                database.create(statement.tableName, columnsList);
                return new SQLExecuteResult("Create table succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult dropTable(DropTableStatement statement) {
            try {
                Database database = Manager.getInstance().getDatabase();
                database.drop(statement.tableName);
                return new SQLExecuteResult("Drop table succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult insert(InsertStatement statement) {
            try {
                Database database = Manager.getInstance().getDatabase();
                Table table = database.getTable(statement.tableName);
                if (statement.columnNameList.size()
                        != statement.valueList.size()) {
                    throw new ColumnValueSizeNotMatchedException();
                }
                ArrayList<Entry> entryList = new ArrayList<>();
                boolean[] covers = new boolean[statement.columnNameList.size()];
                Arrays.fill(covers, false);
                for (Column column: table.columns) {
                    int size = statement.columnNameList.size();
                    boolean assignExist = false;
                    for (int i = 0;i < size;i++) {
                        String columnName = statement.columnNameList.get(i);
                        Comparable value = statement.valueList.get(i);
                        if (column.getName().equals(columnName)) {
                            if (covers[i]) {
                                throw new DuplicateAssignException();
                            }
                            else if (ColumnType.typeCheck(column.getType(), value)) {
                                assignExist = true;
                                entryList.add(new Entry(value));
                                covers[i] = true;
                                break;
                            }
                            else {
                                throw new ColumnTypeNotMatchedException();
                            }
                        }
                    }
                    if (!assignExist) {
                        if (column.isNotNull() || column.isPrimary()) {
                            throw new EmptyKeyException();
                        }
                        else {
                            entryList.add(new Entry(null));
                        }
                    }
                }
                for (boolean cover: covers) {
                    if (!cover) {
                        throw new ColumnDoesNotExistException();
                    }
                }
                table.insert(new Row(entryList));
                return new SQLExecuteResult("Insert operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult delete(DeleteStatement statement) {
            try {
                Database database = Manager.getInstance().getDatabase();
                Table table = database.getTable(statement.tableName);
                QueryResult queryResult = new QueryResult(table);
                ArrayList<Row> row2Delete = queryResult.deleteQuery(statement);
                for (Row row: row2Delete) {
                    table.delete(row);
                }
                return new SQLExecuteResult("Delete operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult update(UpdateStatement statement) {
            try {
                Database database = Manager.getInstance().getDatabase();
                Table table = database.getTable(statement.tableName);
                QueryResult queryResult = new QueryResult(table);
                ArrayList<Row> row2Update = queryResult.updateQuery(statement);
                if (row2Update.size() == 0) {
                    return new SQLExecuteResult("No matched rows to update.", true, false);
                }
                int index = queryResult.columnFind(statement.columnName);
                if (index == -1) {
                    throw new ColumnDoesNotExistException();
                }
                ArrayList<Row> rowUpdated = queryResult.updateRowList(row2Update, index, statement.expression);
                // check if primary key updated
                if (index == table.primaryIndex
                        && !row2Update.get(0).getEntry(index).equals(rowUpdated.get(0).getEntry(index))) {
                    // update primary key
                    if (table.checkRowExist(rowUpdated.get(0).getEntry(index))) {
                        throw new DuplicateKeyException();
                    }
                    else {
                        table.delete(row2Update.get(0));
                        table.insert(rowUpdated.get(0));
                    }
                }
                else {
                    // normal update
                    for (Row row: rowUpdated) {
                        table.update(row);
                    }
                }
                return new SQLExecuteResult("Update operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult select(SelectStatement statement) {
            try {
                Database database = Manager.getInstance().getDatabase();
                ArrayList<Table> tables2Query = new ArrayList<>();
                tables2Query.add(database.getTable(statement.tableQuery.tableNameLeft));
                if (statement.tableQuery.tableNameRight != null) {
                    tables2Query.add(database.getTable(statement.tableQuery.tableNameRight));
                }
                QueryResult queryResult = new QueryResult(tables2Query);

                queryResult.selectQuery(statement);
                System.out.println(queryResult.getAttrList());
                System.out.println(queryResult.getResultRowList());
                return new SQLExecuteResult(
                        "Query succeeds.",
                        queryResult.getAttrList(),
                        queryResult.getResultRowList()
                );
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }
    }
}
