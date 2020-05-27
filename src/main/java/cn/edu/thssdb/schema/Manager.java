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
    private static SQLExecutor sqlExecutor;
    private static Logger logger;
    private static ReentrantReadWriteLock lock;
    private static ArrayList<Session> sessionList;

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    private Manager() {
        // TODO
        databases = new HashMap<>();
        lock = new ReentrantReadWriteLock();
        logger = new Logger();
        sqlExecutor = new SQLExecutor();
        sessionList = new ArrayList<>();
        recover();
    }

    public void addSession(long sessionId) {
        sessionList.add(new Session(sessionId));
    }

    public void deleteSession(long sessionId) {
        for (Session session: sessionList) {
            if (session.sessionId == sessionId) {
                for (ReentrantReadWriteLock lock: session.lockList) {
                    lock.writeLock().unlock();
                }
                sessionList.remove(session);
                break;
            }
        }
    }

    public boolean checkSessionExist(long sessionId) {
        for (Session session: sessionList) {
            if (session.sessionId == sessionId) {
                return true;
            }
        }
        return false;
    }

    public Session getSession(long sessionId) {
        for (Session session: sessionList) {
            if (session.sessionId == sessionId) {
                return session;
            }
        }
        return null;
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
//            if (currentDatabase != null) {
//                Database database = databases.get(currentDatabase);
//                if (database == null) {
//                    System.err.println("Current database is null while trying to persist!");
//                }
//                else {
//                    database.quit();
//                }
//            }
            for (Database database: databases.values()) {
                database.persist();
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

    private static boolean ownLock(ArrayList<ReentrantReadWriteLock> lockList, ReentrantReadWriteLock lock) {
        return lockList.contains(lock);
    }

    private static void lockAll() {
        lock.writeLock().lock();
    }

    private static void lockDatabase(Database database) {
        lock.writeLock().lock();
        database.lock.writeLock().lock();
        lock.writeLock().unlock();
    }

    private static void lockTable(Database database, Table table) {
        lock.writeLock().lock();
        database.lock.writeLock().lock();
        table.lock.writeLock().lock();
        database.lock.writeLock().unlock();
        lock.writeLock().unlock();
    }

    private void createDatabaseIfNotExists(String name) {
        // TODO
        try {
//            lock.writeLock().lock();
            if (databases.get(name) != null) {
                throw new DatabaseAlreadyExistException();
            }
            Database database = new Database(name);
            databases.put(name, database);
        }
        finally {
//            lock.writeLock().unlock();
        }
    }

    private void deleteDatabase(String name, Session session) {
        // TODO
        try {
//            lock.writeLock().lock();
            if (databases.get(name) == null) {
                throw new DatabaseNotExistException();
            }

//            if (currentDatabase != null && currentDatabase.equals(name)) {
//                currentDatabase = null;
//            }

            for (Session session_: sessionList) {
                if (session_ != session && name.equals(session_.currentDatabase)) {
                    throw new DatabaseIsBeingUsedException();
                }
            }
            if (name.equals(session.currentDatabase)) {
                session.currentDatabase = null;
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
        }
        catch (IOException e) {
            System.err.println("Fail to remove database file!");
            e.printStackTrace();
        }
        finally {
//            lock.writeLock().unlock();
        }
    }

    private void switchDatabase(String name, Session session) {
        // TODO
        if (name.equals(session.currentDatabase)) return;
        try {
            lock.writeLock().lock();
            Database database = databases.get(name);
            if (database == null) {
                throw new DatabaseNotExistException();
            }
            if (database.tables == null) {
                database.recover();
            }
            if (session.currentDatabase != null) {
                Database current = databases.get(session.currentDatabase);
                if (current == null) {
                    System.err.println("Current database is null while trying to persist!");
                }
                else {
                    boolean databaseIsBeingUsed = false;
                    for (Session session_: sessionList) {
                        if (session_ != session && current.name.equals(session_.currentDatabase)) {
                            databaseIsBeingUsed = true;
                            break;
                        }
                    }
                    if (!databaseIsBeingUsed) {
                        current.quit();
                    }
                }
            }
            session.currentDatabase = name;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

//    public void quit() {
//        persist();
//        try {
//            lock.writeLock().lock();
//            currentDatabase = null;
//        }
//        finally {
//            lock.writeLock().unlock();
//        }
//
//    }

    private Database getDatabase(Session session) {
        try {
            lock.readLock().lock();
            if (session.currentDatabase == null) {
                throw new DatabaseNotSelectException();
            }
            else {
                return databases.get(session.currentDatabase);
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public List<SQLExecutor.SQLExecuteResult> execute(String sql, long sessionId) {
        Session session = getSession(sessionId);
        if (session == null) {
            List<SQLExecutor.SQLExecuteResult> resultList = new ArrayList<>();
            resultList.add(new SQLExecutor.SQLExecuteResult("Invalid session!", false, false));
            return resultList;
        }
        List<Statement> statementList = sqlExecutor.parseSQL(sql);
        if (statementList == null || statementList.size() == 0) {
            List<SQLExecutor.SQLExecuteResult> resultList = new ArrayList<>();
            resultList.add(new SQLExecutor.SQLExecuteResult("SQL syntax error! Check your statement.", false, false));
            return resultList;
        }
        else {
            return sqlExecutor.executeSQL(statementList, session);
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

        private ArrayList<SQLExecuteResult> executeSQL(List<Statement> statementList, Session session) {
            ArrayList<SQLExecuteResult> resultList = new ArrayList<>();
            for (Statement statement: statementList) {
                System.out.println(statement.get_type());
                switch (statement.get_type()) {
                    case CREATE_DATABASE:
                        resultList.add(createDatabase((CreatDatabaseStatement)statement, session));
                        break;
                    case DROP_DATABASE:
                        resultList.add(dropDatabase((DropDatabaseStatement)statement, session));
                        break;
                    case USE:
                        resultList.add(useDatabase((UseDatabaseStatement)statement, session));
                        break;
                    case CREATE_TABLE:
                        resultList.add(createTable((CreateTableStatement)statement, session));
                        break;
                    case DROP_TABLE:
                        resultList.add(dropTable((DropTableStatement)statement, session));
                        break;
                    case INSERT:
                        resultList.add(insert((InsertStatement)statement, session));
                        break;
                    case SELECT:
                        resultList.add(select((SelectStatement)statement, session));
                        break;
                    case DELETE:
                        resultList.add(delete((DeleteStatement)statement, session));
                        break;
                    case UPDATE:
                        resultList.add(update((UpdateStatement)statement, session));
                        break;
                    default:
                        resultList.add(new SQLExecuteResult("Error: SQL syntax not supported!", false, false));
                        break;
                }
            }
            return resultList;
        }

        private SQLExecuteResult createDatabase(CreatDatabaseStatement statement, Session session) {
            try {
                Manager.getInstance().createDatabaseIfNotExists(statement.databaseName);

                if (!ownLock(session.lockList, lock)) {
                    lockAll();
                    session.lockList.add(lock);
                }

                logger.addCreateDatabase(session.logList, statement.databaseName);

                if (session.autoCommit) {
                    session.lockList.remove(lock);
                    lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                }

                return new SQLExecuteResult("Create database succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult dropDatabase(DropDatabaseStatement statement, Session session) {
            try {
                Manager.getInstance().deleteDatabase(statement.databaseName, session);

                if (!ownLock(session.lockList, lock)) {
                    lockAll();
                    session.lockList.add(lock);
                }

                logger.addDropDatabase(session.logList, statement.databaseName);

                if (session.autoCommit) {
                    session.lockList.remove(lock);
                    lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                }

                return new SQLExecuteResult("Drop database succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult useDatabase(UseDatabaseStatement statement, Session session) {
            try {
                Manager.getInstance().switchDatabase(statement.databaseName, session);
                return new SQLExecuteResult("Database switch to "+statement.databaseName+".", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult createTable(CreateTableStatement statement, Session session) {
            try {
                Database database = Manager.getInstance().getDatabase(session);
                if (database.checkTableExist(statement.tableName)) {
                    throw new TableAlreadyExistException();
                }

                if (!ownLock(session.lockList, database.lock)) {
                    lockDatabase(database);
                    session.lockList.add(database.lock);
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

                logger.addCreateTable(session.logList, database.name, statement.tableName, columnsList);

                if (session.autoCommit) {
                    session.lockList.remove(database.lock);
                    database.lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                }

                return new SQLExecuteResult("Create table succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult dropTable(DropTableStatement statement, Session session) {
            try {
                Database database = Manager.getInstance().getDatabase(session);
                if (!database.checkTableExist(statement.tableName)) {
                    throw new TableNotExistException();
                }

                if (!ownLock(session.lockList, database.lock)) {
                    lockDatabase(database);
                    session.lockList.add(database.lock);
                }

                database.drop(statement.tableName);

                logger.addDropTable(session.logList, database.name, statement.tableName);

                if (session.autoCommit) {
                    session.lockList.remove(database.lock);
                    database.lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                }

                return new SQLExecuteResult("Drop table succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult insert(InsertStatement statement, Session session) {
            try {
                Database database = Manager.getInstance().getDatabase(session);
                Table table = database.getTable(statement.tableName);

                if (!ownLock(session.lockList, table.lock)) {
                    lockTable(database, table);
                    session.lockList.add(table.lock);
                }

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
                Row row = new Row(entryList);
                table.insert(row);

                logger.addInsert(session.logList, database.name, statement.tableName, row);

                if (session.autoCommit) {
                    session.lockList.remove(table.lock);
                    table.lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                }

                return new SQLExecuteResult("Insert operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult delete(DeleteStatement statement, Session session) {
            try {
                Database database = Manager.getInstance().getDatabase(session);
                Table table = database.getTable(statement.tableName);

                if (!ownLock(session.lockList, table.lock)) {
                    lockTable(database, table);
                    session.lockList.add(table.lock);
                }

                QueryResult queryResult = new QueryResult(table);
                ArrayList<Row> row2Delete = queryResult.deleteQuery(statement);
                for (Row row: row2Delete) {
                    table.delete(row);
                }

                logger.addDelete(session.logList, database.name, statement.tableName, row2Delete);

                if (session.autoCommit) {
                    session.lockList.remove(table.lock);
                    table.lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                }

                return new SQLExecuteResult("Delete operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult update(UpdateStatement statement, Session session) {
            try {
                Database database = Manager.getInstance().getDatabase(session);
                Table table = database.getTable(statement.tableName);

                if (!ownLock(session.lockList, table.lock)) {
                    lockTable(database, table);
                    session.lockList.add(table.lock);
                }

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
                        Row rowBefore = row2Update.get(0);
                        table.delete(rowBefore);

                        logger.addDelete(session.logList, database.name, statement.tableName, rowBefore);

                        Row rowAfter = rowUpdated.get(0);
                        table.insert(rowAfter);

                        logger.addInsert(session.logList, database.name, statement.tableName, rowAfter);
                    }
                }
                else {
                    // normal update
                    for (Row row: rowUpdated) {
                        table.update(row);
                    }

                    logger.addUpdate(session.logList, database.name, statement.tableName, rowUpdated);
                }

                if (session.autoCommit) {
                    session.lockList.remove(table.lock);
                    table.lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                }

                return new SQLExecuteResult("Update operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult select(SelectStatement statement, Session session) {
            try {
                Database database = Manager.getInstance().getDatabase(session);
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

    private static class Logger {
        private ReentrantReadWriteLock lock;

        private Logger() {
            lock = new ReentrantReadWriteLock();
        }

        private void addCreateDatabase(ArrayList<String> logList, String databaseName) {
            ArrayList<String> log = new ArrayList<>();
            log.add(Statement.Type.CREATE_DATABASE.toString());
            log.add(databaseName);
            logList.add(String.join("|", log));
        }

        private void addDropDatabase(ArrayList<String> logList, String databaseName) {
            ArrayList<String> log = new ArrayList<>();
            log.add(Statement.Type.DROP_DATABASE.toString());
            log.add(databaseName);
            logList.add(String.join("|", log));
        }

        private void addCreateTable(ArrayList<String> logList, String databaseName, String tableName, ArrayList<Column> columnsList) {
            ArrayList<String> log = new ArrayList<>();
            log.add(Statement.Type.CREATE_TABLE.toString());
            log.add(databaseName);
            log.add(tableName);
            for (Column column: columnsList) {
                log.add(column.toString());
            }
            logList.add(String.join("|", log));
        }

        private void addDropTable(ArrayList<String> logList, String databaseName, String tableName) {
            ArrayList<String> log = new ArrayList<>();
            log.add(Statement.Type.DROP_TABLE.toString());
            log.add(databaseName);
            log.add(tableName);
            logList.add(String.join("|", log));
        }

        private void addInsert(ArrayList<String> logList, String databaseName, String tableName, Row row) {
            ArrayList<String> log = new ArrayList<>();
            log.add(Statement.Type.INSERT.toString());
            log.add(databaseName);
            log.add(tableName);
            log.add(row.toString());
            logList.add(String.join("|", log));
        }

        private void addDelete(ArrayList<String> logList, String databaseName, String tableName, ArrayList<Row> row2Delete) {
            ArrayList<String> log = new ArrayList<>();
            log.add(Statement.Type.DELETE.toString());
            log.add(databaseName);
            log.add(tableName);
            for (Row row: row2Delete) {
                log.add(row.toString());
            }
            logList.add(String.join("|", log));
        }

        private void addDelete(ArrayList<String> logList, String databaseName, String tableName, Row row) {
            ArrayList<String> log = new ArrayList<>();
            log.add(Statement.Type.DELETE.toString());
            log.add(databaseName);
            log.add(tableName);
            log.add(row.toString());
            logList.add(String.join("|", log));
        }

        private void addUpdate(ArrayList<String> logList, String databaseName, String tableName, ArrayList<Row> rowUpdated) {
            ArrayList<String> log = new ArrayList<>();
            log.add(Statement.Type.UPDATE.toString());
            log.add(databaseName);
            log.add(tableName);
            for (Row row: rowUpdated) {
                log.add(row.toString());
            }
            logList.add(String.join("|", log));
        }

        private void commitLog(ArrayList<String> logList) {
            try {
                lock.writeLock().lock();
                File file = new File(Global.DATABASE_DIR+File.separator+"log");
                FileWriter fileWriter = new FileWriter(file, true);
                for (String string: logList) {
                    fileWriter.write(string+'\n');
                }
                fileWriter.flush();
                fileWriter.close();
            }
            catch (IOException ignored) {
                throw new WriteLogException();
            }
            finally {
                lock.writeLock().unlock();
            }
        }
    }
}

class Session {
    long sessionId;
    boolean autoCommit;
    ArrayList<String> logList;
    ArrayList<ReentrantReadWriteLock> lockList;
    String currentDatabase;

    Session(long sessionId) {
        this.sessionId = sessionId;
        autoCommit = true;
        logList = new ArrayList<>();
        lockList = new ArrayList<>();
        currentDatabase = null;
    }
}