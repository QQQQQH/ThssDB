package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public class QueryTable implements Iterator<Row> {
    private Iterator<Row> iterator;
    private Table table;

    QueryTable(Table table) {
        this.table = table;
        iterator = this.table.iterator();
    }

    @Override
    public boolean hasNext() {
        // TODO
        return iterator.hasNext();
    }

    @Override
    public Row next() {
        // TODO
        return iterator.next();
    }

    void refresh() {
        iterator = this.table.iterator();
    }

    void readLock() { this.table.readLock(); }

    void readUnlock() { this.table.readUnlock(); }
}