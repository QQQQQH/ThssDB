package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import javafx.util.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Entry, Row> index;
    private int primaryIndex;

    public Table(String databaseName, String tableName, Column[] columns) {
        // TODO
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));

        // assign to primaryIndex
        this.primaryIndex = 0; // ????!!!!

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

    public void insert(Row row) {
        // TODO
        try {
            lock.writeLock().lock();
            Entry entry = row.getEntries().get(primaryIndex);
            if (index.contains(entry))
                throw new DuplicateKeyException();
            index.put(entry, row);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void delete(Row row) {
        // TODO
        try {
            lock.writeLock().lock();
            Entry entry = row.getEntries().get(primaryIndex);
            if (!index.contains(entry)) {
                throw new KeyNotExistException();
            }
            index.remove(entry);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void update(Row row) {
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

    private void serialize() {
        // TODO
        File dir = new File(databaseName+File.separator+"table");
        if (!dir.exists() && !dir.mkdirs()) {
            System.err.print("Fail to serialize due to mkdirs error!");
            return;
        }
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dir.toString()+File.separator+tableName));
            for (Row row : this) {
                oos.writeObject(row);
            }
            oos.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.err.print("Fail to serialize due to IOException!");
        }
    }

    private ArrayList<Row> deserialize() {
        // TODO
        File file = new File(databaseName+File.separator+"table"+File.separator+tableName);
        if (!file.exists()) {
            return new ArrayList<>();
        }
        ArrayList<Row> rows = new ArrayList<>();
        try {
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            while (fis.available() > 0) {
                rows.add((Row) ois.readObject());
            }
            ois.close();
            fis.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.err.println("Fail to deserialize due to IOException!");
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("Fail to deserialize due to ClassNotFoundException!");
        }
        return rows;
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
