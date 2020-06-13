package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
    ReentrantReadWriteLock lock;
    String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    BPlusTree<Entry, Row> index;
    int primaryIndex;

    public Table(String databaseName, String tableName, ArrayList<Column> columns) {
        // TODO
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = columns;

        // assign to primaryIndex
        int size = this.columns.size();
        boolean primaryExist = false;
        for (int i = 0;i < size;i++) {
            if (this.columns.get(i).isPrimary()) {
                primaryExist = true;
                this.primaryIndex = i;
                break;
            }
        }
        if (!primaryExist) {
            this.columns.get(0).setPrimary();
            this.primaryIndex = 0;
        }
        this.index = new BPlusTree<>();
        this.lock = new ReentrantReadWriteLock();
        recover();
    }

    private void recover() {
        // TODO
        try {
            lock.writeLock().lock();
            ArrayList<Row> rows = deserialize();
            for (Row row: rows) {
                index.put(row.getEntries().get(primaryIndex), row);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    boolean persist() {
        try {
            lock.writeLock().lock();
            return serialize();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    boolean checkRowExist(Entry primary) {
        try {
            lock.readLock().lock();
            return index.contains(primary);
        }
        finally {
            lock.readLock().unlock();
        }

    }

    void insert(Row row) throws DuplicateKeyException {
        // TODO
        try {
            lock.writeLock().lock();
            Entry primary = row.getEntries().get(primaryIndex);
            if (checkRowExist(primary)) {
                throw new DuplicateKeyException();
            }
            index.put(primary, row);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    void delete(Row row) {
        // TODO
        try {
            lock.writeLock().lock();
            Entry primary = row.getEntries().get(primaryIndex);
            if (!checkRowExist(primary)) {
                throw new KeyNotExistException();
            }
            index.remove(primary);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    void update(Row row) {
        // TODO
        try {
            lock.writeLock().lock();
            Entry entry = row.getEntries().get(primaryIndex);
            if (!index.contains(entry))
                throw new KeyNotExistException();
            index.update(entry, row);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private boolean serialize() {
        // TODO
        try {
            File dir = new File(Global.DATABASE_DIR+File.separator+databaseName+File.separator+"data");
            if (!dir.exists() && !dir.mkdirs()) {
                System.err.print("Fail to serialize due to mkdirs error!");
                return false;
            }
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dir.toString()+File.separator+tableName));
            for (Row row : this) {
                oos.writeObject(row);
            }
            oos.close();
            return true;
        }
        catch (IOException e) {
            System.err.println("Fail to serialize due to IOException!");
            return false;
        }
    }

    private ArrayList<Row> deserialize() {
        // TODO
        try {
            File file = new File(Global.DATABASE_DIR+File.separator+databaseName+File.separator+"data"+File.separator+tableName);
            if (!file.exists()) {
                return new ArrayList<>();
            }
            ArrayList<Row> rows = new ArrayList<>();
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            while (fis.available() > 0) {
                rows.add((Row) ois.readObject());
            }
            ois.close();
            fis.close();
            return rows;
        }
        catch (IOException e) {
            System.err.println("Fail to deserialize due to IOException!");
            return new ArrayList<>();
        }
        catch (ClassNotFoundException e) {
            System.err.println("Fail to deserialize due to ClassNotFoundException!");
            return new ArrayList<>();
        }
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Entry, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().getValue();
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return new TableIterator(this);
    }
}
