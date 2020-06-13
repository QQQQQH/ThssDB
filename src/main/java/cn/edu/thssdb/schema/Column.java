package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

import java.util.ArrayList;

public class Column implements Comparable<Column> {
    private String name;
    private ColumnType type;
    private int primary;
    private boolean notNull;
    private int maxLength;

    public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
        this.name = name;
        this.type = type;
        this.primary = primary;
        this.notNull = notNull;
        this.maxLength = maxLength;
    }

    @Override
    public int compareTo(Column e) {
        return name.compareTo(e.name);
    }

    public String getName() { return name; }

    void setPrimary() { primary = 1; }

    boolean isPrimary() { return primary == 1; }

    boolean isNotNull() { return notNull; }

    ArrayList<String> getMetaList() {
        return new ArrayList<String>() {{
            add(name);
            add(type.equals(ColumnType.STRING) ? type.toString()+"("+maxLength+")" : type.toString());
            add(notNull || primary == 1 ? "NO" : "YES");
            add(primary == 1 ? "PRI" : "");
        }};
    }

    public ColumnType getType() { return type; }

    static Column parseColumnDef(String defStr) {
        String[] defListStr = defStr.split(",");
        return new Column(defListStr[0], // name
                ColumnType.valueOf(defListStr[1]),  // ColumnType
                Integer.parseInt(defListStr[2]),  // primary
                defListStr[3].equals("true"), // notNull
                Integer.parseInt(defListStr[4])); // maxLength
    }

    public String toString() {
        return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
    }

}
