package cn.edu.thssdb.schema;

import java.io.Serializable;

public class Entry implements Comparable<Entry>, Serializable {
    private static final long serialVersionUID = -5809782578272943999L;
    public Comparable value;

    public Entry(Comparable value) {
        if (value instanceof String && ((String) value).startsWith("'") && ((String) value).endsWith("'")) {
            this.value = ((String) value).substring(1, ((String) value).length()-1);
        }
        else {
            this.value = value;
        }
    }

    @Override
    public int compareTo(Entry e) {
        return value.compareTo(e.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (this.getClass() != obj.getClass())
            return false;
        Entry e = (Entry) obj;
        return value.equals(e.value);
    }

    public String toString() {
        return value == null ? "null" : value.toString();
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
