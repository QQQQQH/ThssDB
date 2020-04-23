package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableAlreadyExistException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.type.ColumnType;

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
        this.tables = new HashMap<>();
        this.lock = new ReentrantReadWriteLock();
        recover();
    }

    private void persist() {
        // TODO
        File dir = new File(name);
        if (!dir.exists() && !dir.mkdirs()) {
            System.out.println("Fail to persist due to mkdirs error!");
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
                // serialize data !!!
            }
            oos.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.err.print("Fail to persist due to IOException!");
        }
        finally {
            lock.writeLock().unlock();
        }
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

    public void drop(String tableName) {
        // TODO
        try {
            lock.writeLock().lock();
            if (tables.get(tableName) == null) {
                throw new TableNotExistException();
            }
            tables.remove(tableName);
            // remove table file
            File serialFile = new File(name+File.separator+"data"+File.separator+tableName);
            if (serialFile.exists() && !serialFile.delete()) {
                System.err.println("Fail to remove serialization file!");
            }
            File schemaFile = new File(name+File.separator+tableName+"_SCHEMA");
            if (schemaFile.exists() && !schemaFile.delete()) {
                System.err.println("Fail to remove schema file!");
            }
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
        File file = new File(name+File.separator+"TABLES_NAME");
        if (!file.exists()) return;
        try {
            lock.writeLock().lock();
            FileInputStream fis = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fis);
            while (fis.available() > 0) {
                String tableName = (String)ois.readObject();
                File schemaFile = new File(name+File.separator+tableName+"_SCHEMA");
                if (!schemaFile.exists()) {
                    System.err.println("Fail to load table schema!");
                    continue;
                }
                ArrayList<Column> columnsList = new ArrayList<>();
                FileInputStream fisSchema = new FileInputStream(file.toString()+File.separator+"TABLES_NAME");
                ObjectInputStream oisSchema = new ObjectInputStream(fis);
                while (oisSchema.available() > 0) {
                    String schemaStr = (String)oisSchema.readObject();
                    String[] schemaListStr = schemaStr.split(",");
                    columnsList.add(new Column(schemaListStr[0], // name
                            ColumnType.valueOf(schemaListStr[1]),  // ColumnTyoe
                            Integer.parseInt(schemaListStr[2]),  // primary
                            schemaListStr[3].equals("true"), // notNull
                            Integer.parseInt(schemaListStr[2]))); // maxLength
                }
                oisSchema.close();
                fisSchema.close();
                Table table = new Table(name, tableName, (Column[])columnsList.toArray());
                tables.put(tableName, table);
            }
            ois.close();
            fis.close();
        }
        catch (IOException e) {
            e.printStackTrace();
            System.err.println("Fail to recover due to IOException!");
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("Fail to recover due to ClassNotFoundException!");
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void quit() {
        // TODO
        persist();
    }
}
