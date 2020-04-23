package cn.edu.thssdb.benchmark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class Testdb {

    private static final Logger logger = LoggerFactory.getLogger(Testdb.class);

    private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);

    Connection connection;

    public Testdb() throws Exception {
        Class.forName("cn.edu.thssdb.jdbc.JDBCDriver");
        connection = DriverManager.getConnection(
                "jdbc:thssdb://127.0.0.1:6667/test",
                "user",
                "password");
    }

    public void shutdown() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("SHUTDOWN");
        connection.close();
    }

    public synchronized void query(String expression) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(expression);

        dump(resultSet);
        statement.close();
    }

    public synchronized void update(String expression) throws SQLException {
        Statement statement = connection.createStatement();
        if (statement.executeUpdate(expression) == -1) {
            println("Error: " + expression);
        }
        statement.close();
    }

    public static void dump(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        Object object = null;

        for (; resultSet.next(); ) {
            for (int i = 0; i < columnCount; ++i) {
                object = resultSet.getObject(i + 1);
                print(object.toString() + " ");
            }
            println(" ");
        }
    }

    public static void main(String[] args) {
        Testdb db = null;
        long startTime;
        long endTime;

        try {
            db = new Testdb();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return;
        }

        try {
            startTime = System.currentTimeMillis();
            db.update(
                    "CREATE TABLE sample_table ( id INTEGER IDENTITY, str_col VARCHAR(256), num_col INTEGER)");
            endTime = System.currentTimeMillis();
            println("CREATE TABLE costs " + (endTime - startTime) + " ms.");
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }

        try {
            startTime = System.currentTimeMillis();
            db.update(
                    "INSERT INTO sample_table(str_col,num_col) VALUES('Ford', 100)");
            db.update(
                    "INSERT INTO sample_table(str_col,num_col) VALUES('Toyota', 200)");
            db.update(
                    "INSERT INTO sample_table(str_col,num_col) VALUES('Honda', 300)");
            db.update(
                    "INSERT INTO sample_table(str_col,num_col) VALUES('GM', 400)");
            endTime = System.currentTimeMillis();
            println("INSERT costs " + (endTime - startTime) + " ms.");

            startTime = System.currentTimeMillis();
            db.query("SELECT * FROM sample_table WHERE num_col < 250");
            endTime = System.currentTimeMillis();
            println("QUERY costs " + (endTime - startTime) + " ms.");

            db.shutdown();
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
    }

    static void print(String msg) {
        SCREEN_PRINTER.print(msg);
    }

    static void println() {
        SCREEN_PRINTER.println();
    }

    static void println(String msg) {
        SCREEN_PRINTER.println(msg);
    }
}
