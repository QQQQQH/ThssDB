package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseAlreadyExistException;
import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.server.ThssDB;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
    private HashMap<String, Database> databases;
    private String currentDatabase;
    private static ReentrantReadWriteLock lock;

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public Manager() {
        // TODO
        this.databases = new HashMap<>();
        this.currentDatabase = null;
        lock = new ReentrantReadWriteLock();
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

    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager();

        private ManagerHolder() {

        }
    }
}
