package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseAlreadyExistException;
import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.parser.*;
import cn.edu.thssdb.server.ThssDB;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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

    private void createDatabaseIfNotExists(String name) {
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

    private void deleteDatabase(String name) {
        // TODO
        try {
            lock.writeLock().lock();
            if (databases.get(name) == null) {
                throw new DatabaseNotExistException();
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

    public void switchDatabase(String name) {
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

    public void execute(String sql) {
        sqlExecutor.parseAndExecute(sql);
    }

    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager();

        private ManagerHolder() {

        }
    }

    private static class SQLExecutor {
        private SQLExecutor() {}

        @SuppressWarnings("unchecked")
        private void parseAndExecute(String sql) {
            CharStream input = CharStreams.fromString(sql);
            SQLLexer lexer = new SQLLexer(input);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            SQLParser parser = new SQLParser(tokens);
            ParseTree tree = parser.parse();
            MySQLVisitor visitor = new MySQLVisitor();
            List<Statement> statementList =  (ArrayList<Statement>) visitor.visit(tree);
        }
    }
}
