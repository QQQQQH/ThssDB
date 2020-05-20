package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.List;

class MetaInfo {

    private String tableName;
    private List<Column> columns;

    MetaInfo(String tableName, ArrayList<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    int columnFind(String name) {
        // TODO
        int index = -1;
        int leftSize = columns.size();
        for (int i = 0;i < leftSize;i++) {
            if (columns.get(i).getName().equals(name)) {
                index = i;
                break;
            }
        }
        return index;
    }

    int getColumnSize() { return columns.size(); }

    String getColumnName(int index) { return columns.get(index).getName(); }

    String getTableName() { return tableName; }
}