package cn.edu.thssdb.utils;

public class Global {
    public static int fanout = 129;

    public static int SUCCESS_CODE = 0;
    public static int FAILURE_CODE = -1;

    public static String DEFAULT_SERVER_HOST = "127.0.0.1";
    public static int DEFAULT_SERVER_PORT = 6667;

    public static String CLI_PREFIX = "ThssDB>";
    public static final String SHOW_TIME = "show time;";
    public static final String QUIT = "quit;";

    public static final String S_URL_INTERNAL = "jdbc:default:connection";

    // custom
    public static final String CONNECT = "connect;";
    public static final String DISCONNECT = "disconnect;";
    public static final String SET_AUTO_COMMIT_TRUE = "set auto commit true;";
    public static final String SET_AUTO_COMMIT_FALSE = "set auto commit false;";
    public static final String BEGIN_TRANSACTION = "begin transaction;";
    public static final String COMMIT = "commit;";

    public static final String USERNAME = "SA";
    public static final String PASSWORD = "";

    public static final String DATABASE_DIR = "db";

    public static final int FLUSH_THRESHOLED = 5;
}
