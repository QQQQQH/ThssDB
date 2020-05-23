package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.schema.Manager.SQLExecutor.SQLExecuteResult;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ThssDB {

    private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

    private static IServiceHandler handler;
    private static IService.Processor processor;
    private static TServerSocket transport;
    private static TServer server;

    private static Manager manager;

    private static long sessionCnt;
    private static List<Long> sessionList;

    public static ThssDB getInstance() {
        return ThssDBHolder.INSTANCE;
    }

    public static void main(String[] args) {
        sessionCnt = 0;
        sessionList = new ArrayList<>();
        manager = Manager.getInstance();
        ThssDB server = ThssDB.getInstance();
        server.start();
    }

    private void start() {
        handler = new IServiceHandler();
        processor = new IService.Processor(handler);
        Runnable setup = () -> setUp(processor);
        new Thread(setup).start();
    }

    private static void setUp(IService.Processor processor) {
        try {
            transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
            server = new TSimpleServer(new TServer.Args(transport).processor(processor));
            logger.info("Starting ThssDB ...");
            server.serve();
        } catch (TTransportException e) {
            logger.error(e.getMessage());
        }
    }

    public long setupSession() {
        long sessionId = sessionCnt++;
        sessionList.add(sessionId);
        return sessionId;
    }

    public void clearSession(long sessionId) {
        manager.quit();
        sessionList.remove(sessionId);
    }

    public boolean checkSession(long sessionId) {
        return sessionList.contains(sessionId);
    }

    public SQLExecuteResult execute(String sql) {
        List<SQLExecuteResult> resultList = manager.execute(sql);
        return resultList.get(0);
    }

    private static class ThssDBHolder {
        private static final ThssDB INSTANCE = new ThssDB();

        private ThssDBHolder() {

        }
    }
}
