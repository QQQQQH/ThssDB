package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Manager.SQLExecutor.SQLExecuteResult;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.Date;

public class IServiceHandler implements IService.Iface {

    @Override
    public GetTimeResp getTime(GetTimeReq req) throws TException {
        GetTimeResp resp = new GetTimeResp();
        resp.setTime(new Date().toString());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ConnectResp connect(ConnectReq req) throws TException {
        // TODO
        ConnectResp resp = new ConnectResp();
        if (req.username.equals(Global.USERNAME) && req.password.equals(Global.PASSWORD)) {
            resp.setSessionId(ThssDB.getInstance().setupSession());
            resp.setStatus(new Status(Global.SUCCESS_CODE));
        }
        else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
        }
        return resp;
    }

    @Override
    public DisconnetResp disconnect(DisconnetReq req) throws TException {
        // TODO
        DisconnetResp resp = new DisconnetResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        ThssDB.getInstance().clearSession(req.getSessionId());
        return resp;
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
        // TODO
        ThssDB thssDB = ThssDB.getInstance();
        ExecuteStatementResp resp = new ExecuteStatementResp();
        SQLExecuteResult result = thssDB.execute(req.getStatement(), req.sessionId);
        resp.setMsg(result.getMessage());
        resp.setStatus(new Status(result.isIsSucceed() ? Global.SUCCESS_CODE : Global.FAILURE_CODE));
        resp.setIsAbort(result.isIsAbort());
        resp.setHasResult(result.isHasResult());
        if (result.isHasResult()) {
            resp.setColumnsList(result.getColumnList());
            resp.setRowList(result.getRowList());
        }
        return resp;
    }

    @Override
    public SetAutoCommitResp setAutoCommit(SetAutoCommitReq req) throws TException {
        ThssDB thssDB = ThssDB.getInstance();
        SetAutoCommitResp resp = new SetAutoCommitResp();
        int result = thssDB.setAutoCommit(req.isAutoCommit(), req.getSessionId());
        if (result == 1) {
            resp.setStatus(new Status(Global.SUCCESS_CODE));
            if (req.isAutoCommit()) {
                resp.setMsg("Enable auto commit.");
            }
            else {
                resp.setMsg("Disable auto commit.");
            }
        }
        else if (result == 2) {
            resp.setStatus(new Status(Global.FAILURE_CODE));
            resp.setMsg("Current transaction hasn't been committed.");
        }
        else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
            resp.setMsg("Invalid session!");
        }
        return resp;
    }

    @Override
    public BeginTransactionResp beginTransaction(BeginTransactionReq req) throws TException {
        ThssDB thssDB = ThssDB.getInstance();
        BeginTransactionResp resp = new BeginTransactionResp();
        int result = thssDB.beginTransaction(req.sessionId);
        if (result == 1) {
            resp.setStatus(new Status(Global.SUCCESS_CODE));
            resp.setMsg("Transaction begins.");
        }
        else if (result == 2) {
            resp.setStatus(new Status(Global.FAILURE_CODE));
            resp.setMsg("Current transaction hasn't been committed.");
        }
        else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
            resp.setMsg("Invalid session!");
        }
        return resp;
    }

    @Override
    public CommitResp commit(CommitReq req) throws TException {
        ThssDB thssDB = ThssDB.getInstance();
        CommitResp resp = new CommitResp();
        int result = thssDB.commit(req.getSessionId());
        if (result == 1) {
            resp.setStatus(new Status(Global.SUCCESS_CODE));
            resp.setMsg("Commit Succeeds.");
        }
        else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
            resp.setMsg("Invalid session!");
        }
        return resp;
    }
}
