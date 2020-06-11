namespace java cn.edu.thssdb.rpc.thrift

struct Status {
  1: required i32 code;
  2: optional string msg;
}

struct GetTimeReq {
}

struct ConnectReq{
  1: required string username
  2: required string password
}

struct ConnectResp{
  1: required Status status
  2: required i64 sessionId
}

struct DisconnectReq{
  1: required i64 sessionId
}

struct DisconnectResp{
  1: required Status status
}

struct GetTimeResp {
  1: required string time
  2: required Status status
}

struct ExecuteStatementReq {
  1: required i64 sessionId
  2: required string statement
}

struct ExecuteStatementResp {
  1: required Status status
  2: required bool isAbort
  3: required bool hasResult
  // only for query
  4: optional list<string> columnsList
  5: optional list<list<string>> rowList
  6: required string msg;
}

struct SetAutoCommitReq {
  1: required i64 sessionId
  2: required bool autoCommit
}

struct SetAutoCommitResp {
  1: required Status status
  2: required string msg
}

struct BeginTransactionReq {
  1: required i64 sessionId
}

struct BeginTransactionResp {
  1: required Status status
  2: required string msg
}

struct CommitReq {
  1: required i64 sessionId
}

struct CommitResp {
  1: required Status status
  2: required string msg
}

service IService {
  GetTimeResp getTime(1: GetTimeReq req);
  ConnectResp connect(1: ConnectReq req);
  DisconnectResp disconnect(1: DisconnectReq req);
  ExecuteStatementResp executeStatement(1: ExecuteStatementReq req);
  SetAutoCommitResp setAutoCommit(1: SetAutoCommitReq req);
  BeginTransactionResp beginTransaction(1: BeginTransactionReq req);
  CommitResp commit(1: CommitReq req);
}
