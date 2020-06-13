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
    private static HashMap<String, Database> databases;
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
        logger.redoLog();
    }

    public static void addSession(long sessionId) {
        sessionList.add(new Session(sessionId));
    }

    public static void deleteSession(long sessionId) {
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

    private static Session getSession(long sessionId) {
        for (Session session: sessionList) {
            if (session.sessionId == sessionId) {
                return session;
            }
        }
        return null;
    }

    public static int setAutoCommit(boolean autoCommit, long sessionId) {
        Session session = getSession(sessionId);
        if (session == null) {
            return 0;
        }
        else {
            if (session.inTransaction) {
                return 2;
            }
            session.autoCommit = autoCommit;
            return 1;
        }
    }

    public static int beginTransaction(long sessionId) {
        Session session = getSession(sessionId);
        if (session == null) {
            return 0;
        }
        else {
            if (session.inTransaction) {
                return 2;
            }
            session.inTransaction = true;
            return 1;
        }
    }

    public static int commit(long sessionId) {
        Session session = getSession(sessionId);
        if (session == null) {
            return 0;
        }
        else {
            for (ReentrantReadWriteLock lock: session.lockList) {
                lock.writeLock().unlock();
            }
            session.lockList.clear();
            logger.commitLog(session.logList);
            session.logList.clear();
            session.inTransaction = false;
            return 1;
        }
    }

    private static boolean persist() {
        try {
            lockAll();
            File dir = new File(Global.DATABASE_DIR);
            if (dir.exists()) {
                Path databaseDirector = Paths.get(dir.toString());
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
            if (!dir.mkdirs()) {
                System.err.println("Fail to persist due to mkdirs error!");
                return false;
            }
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dir.toString()+File.separator+"DATABASES_NAME"));
            for (String databaseName: databases.keySet()) {
                oos.writeObject(databaseName);
            }
            for (Database database: databases.values()) {
                if (!database.persist()) {
                    return false;
                }
            }
            return true;
        }
        catch (FileNotFoundException e) {
            System.err.print("Fail to persist manager due to FileNotFoundException!");
            return false;
        }
        catch (IOException e) {
            System.err.print("Fail to persist manager due to IOException!");
            return false;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private static void recover() {
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

    private static boolean notOwnLock(ArrayList<ReentrantReadWriteLock> lockList, ReentrantReadWriteLock lock) {
        return !lockList.contains(lock);
    }

    private static void lockAll() {
        lock.writeLock().lock();
        // make sure all the child locks released by other transaction
        for (Database database: databases.values()) {
            if (database == null) continue;
            database.lock.writeLock().lock();
            for (Table table: database.tables.values()) {
                if (table == null) continue;
                table.lock.writeLock().lock();
                table.lock.writeLock().unlock();
            }
            database.lock.writeLock().unlock();
        }
    }

    private static void lockDatabase(Database database) {
        lock.writeLock().lock();
        database.lock.writeLock().lock();
        // make sure the child locks of the database released by other transaction
        for (Table table: database.tables.values()) {
            if (table == null) continue;
            table.lock.writeLock().lock();
            table.lock.writeLock().unlock();
        }
        lock.writeLock().unlock();
    }

    private static void lockTable(Database database, Table table) {
        lock.writeLock().lock();
        database.lock.writeLock().lock();
        table.lock.writeLock().lock();
        database.lock.writeLock().unlock();
        lock.writeLock().unlock();
    }

    private static void createDatabaseIfNotExists(String name) {
        // TODO
        if (databases.get(name) != null) {
            throw new DatabaseAlreadyExistException();
        }
        databases.put(name, new Database(name));
    }

    private static void deleteDatabase(String name, Session session) {
        // TODO
        if (databases.get(name) == null) {
            throw new DatabaseNotExistException();
        }

        for (Session session_: sessionList) {
            if (session_ != session && name.equals(session_.currentDatabase)) {
                throw new DatabaseIsBeingUsedException();
            }
        }
        if (session != null && name.equals(session.currentDatabase)) {
            session.currentDatabase = null;
        }

        databases.remove(name);
    }

    private static void switchDatabase(String name, Session session) {
        // TODO
        if (name.equals(session.currentDatabase)) return;
        Database database = databases.get(name);
        if (database == null) {
            throw new DatabaseNotExistException();
        }
        if (database.tables.isEmpty()) {
            database.recover();
        }
        session.currentDatabase = name;

    }

    private static Database getDatabase(Session session) {
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

    private static Database getDatabase(String name) {
        try {
            lock.readLock().lock();
            Database database = databases.get(name);
            if (database == null) {
                throw new DatabaseNotExistException();
            }
            return database;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public static List<SQLExecutor.SQLExecuteResult> execute(String sql, long sessionId) {
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
            ArrayList<SQLExecutor.SQLExecuteResult> resultList = sqlExecutor.executeSQL(statementList, session);
            if (resultList.size() == 0) {
                resultList.add(new SQLExecutor.SQLExecuteResult("SQL syntax error! Check your statement.", false, false));
            }
            return resultList;
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
                if (statement == null) {
                    continue;
                }
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
                    case SHOW_META:
                        resultList.add(showTable((ShowMetaStatement)statement, session));
                    default:
                        resultList.add(new SQLExecuteResult("Error: SQL syntax not supported!", false, false));
                        break;
                }
            }
            return resultList;
        }

        private SQLExecuteResult createDatabase(CreatDatabaseStatement statement, Session session) {
            try {
                session.inTransaction = true;
                if (notOwnLock(session.lockList, lock)) {
                    lockAll();
                    session.lockList.add(lock);
                }

                createDatabaseIfNotExists(statement.databaseName);

                logger.addCreateDatabase(session.logList, statement.databaseName);

                if (session.autoCommit) {
                    session.lockList.remove(lock);
                    lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                    session.inTransaction = false;
                }

                return new SQLExecuteResult("Create database succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult dropDatabase(DropDatabaseStatement statement, Session session) {
            try {
                session.inTransaction = true;
                if (notOwnLock(session.lockList, lock)) {
                    lockAll();
                    session.lockList.add(lock);
                }

                deleteDatabase(statement.databaseName, session);

                logger.addDropDatabase(session.logList, statement.databaseName);

                if (session.autoCommit) {
                    session.lockList.remove(lock);
                    lock.writeLock().unlock();
                    logger.commitLog(session.logList);
                    session.logList.clear();
                    session.inTransaction = false;
                }

                return new SQLExecuteResult("Drop database succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult useDatabase(UseDatabaseStatement statement, Session session) {
            try {
                switchDatabase(statement.databaseName, session);
                return new SQLExecuteResult("Database switch to "+statement.databaseName+".", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult createTable(CreateTableStatement statement, Session session) {
            try {
                Database database = getDatabase(session);
                if (database.checkTableExist(statement.tableName)) {
                    throw new TableAlreadyExistException();
                }

                session.inTransaction = true;
                if (notOwnLock(session.lockList, database.lock)) {
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
                    session.inTransaction = false;
                }

                return new SQLExecuteResult("Create table succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult dropTable(DropTableStatement statement, Session session) {
            try {
                Database database = getDatabase(session);
                if (!database.checkTableExist(statement.tableName)) {
                    throw new TableNotExistException();
                }

                session.inTransaction = true;
                if (notOwnLock(session.lockList, database.lock)) {
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
                    session.inTransaction = false;
                }

                return new SQLExecuteResult("Drop table succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult insert(InsertStatement statement, Session session) {
            try {
                Database database = getDatabase(session);
                Table table = database.getTable(statement.tableName);

                session.inTransaction = true;
                if (notOwnLock(session.lockList, table.lock)) {
                    lockTable(database, table);
                    session.lockList.add(table.lock);
                }

                int size = statement.columnNameList.size();
                if ((size != 0 && size != statement.valueList.size()) ||
                        (size == 0 && statement.valueList.size() != table.columns.size())) {
                    throw new ColumnValueSizeNotMatchedException();
                }
                ArrayList<Entry> entryList = new ArrayList<>();
                boolean[] covers = new boolean[statement.valueList.size()];
                Arrays.fill(covers, false);
                int columnsCnt = table.columns.size();
                for (int i = 0;i < columnsCnt;i++) {
                    Column column = table.columns.get(i);
                    if (size == 0) {
                        Comparable value = statement.valueList.get(i);
                        if (ColumnType.typeCheck(column.getType(), value)) {
                            entryList.add(new Entry(value));
                            covers[i] = true;
                        }
                        else {
                            throw new ColumnTypeNotMatchedException();
                        }
                        continue;
                    }
                    boolean assignExist = false;
                    for (int j = 0;j < size;j++) {
                        String columnName = statement.columnNameList.get(j);
                        Comparable value = statement.valueList.get(j);
                        if (column.getName().equals(columnName)) {
                            if (covers[j]) {
                                throw new DuplicateAssignException();
                            }
                            else if (ColumnType.typeCheck(column.getType(), value)) {
                                assignExist = true;
                                entryList.add(new Entry(value));
                                covers[j] = true;
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
                    session.inTransaction = false;
                }

                return new SQLExecuteResult("Insert operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult delete(DeleteStatement statement, Session session) {
            try {
                Database database = getDatabase(session);
                Table table = database.getTable(statement.tableName);

                session.inTransaction = true;
                if (notOwnLock(session.lockList, table.lock)) {
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
                    session.inTransaction = false;
                }

                return new SQLExecuteResult("Delete operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult update(UpdateStatement statement, Session session) {
            try {
                Database database = getDatabase(session);
                Table table = database.getTable(statement.tableName);

                session.inTransaction = true;
                if (notOwnLock(session.lockList, table.lock)) {
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
                    session.inTransaction = false;
                }

                return new SQLExecuteResult("Update operation succeeds.", true, false);
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }

        private SQLExecuteResult select(SelectStatement statement, Session session) {
            try {
                Database database = getDatabase(session);
                ArrayList<Table> tables2Query = new ArrayList<>();
                tables2Query.add(database.getTable(statement.tableQuery.tableNameLeft));
                if (statement.tableQuery.tableNameRight != null) {
                    tables2Query.add(database.getTable(statement.tableQuery.tableNameRight));
                }
                QueryResult queryResult = new QueryResult(tables2Query);

                queryResult.selectQuery(statement);
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

        private SQLExecuteResult showTable(ShowMetaStatement statement, Session session) {
            try {
                Database database = getDatabase(session);
                Table table = database.getTable(statement.tableName);

                List<List<String>> schemaList = new ArrayList<>();
                for (Column column: table.columns) {
                    schemaList.add(column.getMetaList());
                }

                return new SQLExecuteResult(
                        "Show schema succeeds.",
                        new ArrayList<String>(){{
                            add("Field");
                            add("Type");
                            add("Null");
                            add("Key");
                        }},
                        schemaList
                );
            }
            catch (Exception e) {
                return new SQLExecuteResult(e.getMessage(), false, false);
            }
        }
    }

    private static class Logger {
        private ReentrantReadWriteLock lock;
        private int logCnt;

        private Logger() {
            lock = new ReentrantReadWriteLock();
            logCnt = 0;
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
            if (row2Delete.size() == 0) {
                return;
            }
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
            if (rowUpdated.size() == 0) {
                return;
            }
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
                File dir = new File(Global.DATABASE_DIR);
                if (!dir.exists() && !dir.mkdirs()) {
                    System.err.println("Fail to write log due to mkdirs error!");
                    return;
                }
                FileWriter fileWriter = new FileWriter(dir.toString()+File.separator+"log", true);
                for (String string: logList) {
                    fileWriter.write(string+'\n');
                    logCnt++;
                }
                fileWriter.flush();
                fileWriter.close();
                if (logCnt >= Global.FLUSH_THRESHOLED && persist()) {
                    logCnt = 0;
                    File logFile = new File(Global.DATABASE_DIR+File.separator+"log");
                    if (logFile.exists()) {
                        logFile.delete();
                    }
                }
            }
            catch (IOException ignored) {
                throw new WriteLogException();
            }
            finally {
                lock.writeLock().unlock();
            }
        }

        private void redoLog() {
            try {
                lock.writeLock().lock();
                File file = new File(Global.DATABASE_DIR+File.separator+"log");
                if (!file.exists()) {
                    return;
                }
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = reader.readLine()) != null) {
                    logCnt++;
                    String[] log = line.split("\\|");
                    switch(Statement.Type.valueOf(log[0])) {
                        case CREATE_DATABASE:
                            redoCreateDatabase(log);
                            break;
                        case DROP_DATABASE:
                            redoDropDatabase(log);
                            break;
                        case CREATE_TABLE:
                            redoCreateTable(log);
                            break;
                        case DROP_TABLE:
                            redoDropTable(log);
                            break;
                        case INSERT:
                            redoInsert(log);
                            break;
                        case DELETE:
                            redoDelete(log);
                            break;
                        case UPDATE:
                            redoUpdate(log);
                            break;
                        default:
                            System.err.println("Error: unknown log type!");
                            break;
                    }
                }
            }
            catch (IOException ignored) {}
            finally {
                lock.writeLock().unlock();
            }
        }

        private void redoCreateDatabase(String[] log) {
            try {
                Manager.createDatabaseIfNotExists(log[1]);
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        private void redoDropDatabase(String[] log) {
            try {
                Manager.deleteDatabase(log[1], null);
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        private void redoCreateTable(String[] log) {
            try {
                Database database = Manager.getDatabase(log[1]);
                ArrayList<Column> columnsList = new ArrayList<>();
                for (int i = 3;i < log.length;i++) {
                    columnsList.add(Column.parseColumnDef(log[i]));
                }
                database.create(log[2], columnsList);
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        private void redoDropTable(String[] log) {
            try {
                Database database = Manager.getDatabase(log[1]);
                database.drop(log[2]);
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        private void redoInsert(String[] log) {
            try {
                Database database = Manager.getDatabase(log[1]);
                Table table = database.getTable(log[2]);
                table.insert(Row.parseRowDef(log[3], table.columns));
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        private void redoDelete(String[] log) {
            try {
                Database database = Manager.getDatabase(log[1]);
                Table table = database.getTable(log[2]);
                table.delete(Row.parseRowDef(log[3], table.columns));
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        private void redoUpdate(String[] log) {
            try {
                Database database = Manager.getDatabase(log[1]);
                Table table = database.getTable(log[2]);
                for (int i = 3;i < log.length;i++) {
                    table.update(Row.parseRowDef(log[i], table.columns));
                }
            }
            catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}

class Session {
    long sessionId;
    boolean autoCommit;
    boolean inTransaction;
    ArrayList<String> logList;
    ArrayList<ReentrantReadWriteLock> lockList;
    String currentDatabase;

    Session(long sessionId) {
        this.sessionId = sessionId;
        autoCommit = true;
        inTransaction = false;
        logList = new ArrayList<>();
        lockList = new ArrayList<>();
        currentDatabase = null;
    }
}