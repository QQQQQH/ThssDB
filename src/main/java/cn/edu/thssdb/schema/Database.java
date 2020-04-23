package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableAlreadyExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

    private String name;
    private HashMap<String, Table> tables;
    ReentrantReadWriteLock lock;

    public Database(String name) {
        this.name = name;
        this.tables = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        recover();
    }

    private void persist() {
        // TODO
    }

    public void create(String name, Column[] columns) {
        // TODO
        try {
            lock.writeLock().lock();
            if (tables.get(name) != null) {
                throw new TableAlreadyExistException();
            }
            Table table = new Table(this.name, name, columns);
            tables.put(name, table);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void drop(String name) {
        // TODO
        try {
            lock.writeLock().lock();
            if (tables.get(name) == null) {
                throw new TableNotExistException();
            }
            tables.remove(name);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public String select(QueryTable[] queryTables) {
        // TODO
        QueryResult queryResult = new QueryResult(queryTables);
        return null;
    }

    private void recover() {
        // TODO
    }

    public void quit() {
        // TODO
    }
}
