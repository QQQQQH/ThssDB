package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.ColumnDoesNotExistException;
import cn.edu.thssdb.exception.TableAlreadyExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.parser.Statement.ColumnDef;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.parser.Statement.ColumnType.Type;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

    private String name;
    private HashMap<String, Table> tables;
    ReentrantReadWriteLock lock;

    public Database(String name) {
        this.name = name;

        lock = new ReentrantReadWriteLock();
        tables = null;
    }

    private void persist() {
        // TODO
        if (tables == null) {
            return;
        }
        File dir = new File(Global.DATABASE_DIR+File.separator+name);
        if (!dir.exists() && !dir.mkdirs()) {
            System.err.println("Fail to persist database due to mkdirs error!");
            return;
        }
        try {
            lock.writeLock().lock();
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(dir.toString()+File.separator+"TABLES_NAME"));
            for (String tableName: tables.keySet()) {
                oos.writeObject(tableName);
                ObjectOutputStream oosSchema = new ObjectOutputStream(new FileOutputStream(dir.toString()+File.separator+tableName+"_SCHEMA"));
                for (Column c: tables.get(tableName).columns) {
                    oosSchema.writeObject(c.toString());
                }
                oosSchema.close();
                Table table = tables.get(tableName);
                if (table == null) {
                    System.err.println("Table is null in index while trying to persist.");
                }
                else {
                    table.serialize();
                }
            }
            oos.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.err.print("Fail to persist database due to IOException!");
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    void recover() {
        // TODO
        tables = new HashMap<>();
        File file = new File(Global.DATABASE_DIR+File.separator+name+File.separator+"TABLES_NAME");
        if (!file.exists()) return;
        try {
            lock.writeLock().lock();
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            while (fis.available() > 0) {
                String tableName = (String)ois.readObject();
                File schemaFile = new File(Global.DATABASE_DIR+File.separator+name+File.separator+tableName+"_SCHEMA");
                if (!schemaFile.exists()) {
                    System.err.println("Fail to load table schema!");
                    continue;
                }
                ArrayList<Column> columnsList = new ArrayList<>();
                FileInputStream fisSchema = new FileInputStream(schemaFile.toString());
                ObjectInputStream oisSchema = new ObjectInputStream(fisSchema);
                while (fisSchema.available() > 0) {
                    String schemaStr = (String)oisSchema.readObject();
                    String[] schemaListStr = schemaStr.split(",");
                    columnsList.add(new Column(schemaListStr[0], // name
                            ColumnType.valueOf(schemaListStr[1]),  // ColumnType
                            Integer.parseInt(schemaListStr[2]),  // primary
                            schemaListStr[3].equals("true"), // notNull
                            Integer.parseInt(schemaListStr[2]))); // maxLength
                }
                oisSchema.close();
                fisSchema.close();
                tables.put(tableName, new Table(name, tableName, columnsList));
            }
            ois.close();
            fis.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.err.println("Fail to recover database due to IOException!");
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("Fail to recover database due to ClassNotFoundException!");
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    boolean checkTableExist(String tableName) {
        return tables.get(tableName) != null;
    }

    void create(String tableName, ArrayList<Column> columns)
            throws TableAlreadyExistException {
        try {
            lock.writeLock().lock();
            if (checkTableExist(tableName)) {
                throw new TableAlreadyExistException();
            }
            Table table = new Table(this.name, tableName, columns);
            tables.put(tableName, table);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    void drop(String tableName) throws TableNotExistException {
        // TODO
        try {
            lock.writeLock().lock();
            if (tables.get(tableName) == null) {
                throw new TableNotExistException();
            }
            tables.remove(tableName);
            // remove table file
            File serialFile = new File(Global.DATABASE_DIR+File.separator+name+File.separator+"data"+File.separator+tableName);
            if (serialFile.exists() && !serialFile.delete()) {
                System.err.println("Fail to remove serialization file!");
            }
            File schemaFile = new File(Global.DATABASE_DIR+File.separator+name+File.separator+tableName+"_SCHEMA");
            if (schemaFile.exists() && !schemaFile.delete()) {
                System.err.println("Fail to remove schema file!");
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    Table getTable(String tableName) throws TableNotExistException {
        try {
            lock.writeLock().lock();
            Table table = tables.get(tableName);
            if (table == null) {
                throw new TableNotExistException();
            }
            return table;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    void quit() {
        // TODO
        persist();
        tables = null;
    }
}
