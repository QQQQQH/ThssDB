package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.ColumnValueSizeNotMatchedException;

import java.beans.Encoder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;
    protected ArrayList<Entry> entries;

    public Row() {
        this.entries = new ArrayList<>();
    }

    public Row(ArrayList<Entry> entryList) { this.entries = entryList; }

    public Row(Entry[] entries) {
        this.entries = new ArrayList<>(Arrays.asList(entries));
    }

    public ArrayList<Entry> getEntries() {
        return entries;
    }

    public Entry getEntry(int index) { return entries.get(index); }

    public void appendEntries(ArrayList<Entry> entries) {
        this.entries.addAll(entries);
    }

    public String toString() {
//        if (entries == null)
//            return "EMPTY";
//        StringJoiner sj = new StringJoiner(",");
//        for (Entry e : entries)
//            sj.add(e.toString());
//        return sj.toString();
        ArrayList<String> s = new ArrayList<>();
        for (Entry e : entries)
            s.add(e.toString());
        return String.join(",", s);
    }

    public static Row parseRowDef(String attrStr, ArrayList<Column> columnsList) {
        String[] attrListStr = attrStr.split(",");
        if (attrListStr.length != columnsList.size()) {
            throw new ColumnValueSizeNotMatchedException();
        }
        ArrayList<Entry> entryList = new ArrayList<>();
        for (int i = 0;i < attrListStr.length;i++) {
            switch (columnsList.get(i).getType()) {
                case INT:
                case LONG:
                    entryList.add(new Entry(Long.valueOf(attrListStr[i])));
                    break;
                case FLOAT:
                case DOUBLE:
                    entryList.add(new Entry(Double.valueOf(attrListStr[i])));
                    break;
                case STRING:
                    entryList.add(new Entry(attrListStr[i]));
                    break;
                default:
                    break;
            }
        }
        return new Row(entryList);
    }
}
