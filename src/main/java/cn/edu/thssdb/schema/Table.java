package cn.edu.thssdb.schema;

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
    }

    private void recover() {
        // TODO
    }

    public void insert(Row row) {
        // TODO
        Entry entry = row.getEntries().get(primaryIndex);
        if (index.contains(entry))
            throw new IllegalArgumentException("row already exists!");
        index.put(entry, row);
    }

    public void delete(Row row) {
        // TODO
        Entry entry = row.getEntries().get(primaryIndex);
        if (!index.contains(entry))
            throw new IllegalArgumentException("row not exists!");
        index.remove(entry);
    }

    public void update(Row row) {
        // TODO
        Entry entry = row.getEntries().get(primaryIndex);
        if (!index.contains(entry))
            throw new IllegalArgumentException("row not exists!");
        index.update(entry, row);
    }

    private void serialize() throws IOException {
        // TODO
        ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("table"));
        for (Row row : this) {
            oos.writeObject(row);
        }
        oos.close();
    }

    private ArrayList<Row> deserialize() throws IOException, ClassNotFoundException {
        // TODO
        File file = new File("table");
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(fis);
        ArrayList<Row> rows = new ArrayList<Row>();
        while (fis.available() > 0) {
            rows.add((Row) ois.readObject());
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
