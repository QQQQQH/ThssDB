namespace java cn.edu.thssdb.rpc.thrift

struct Status {
  1: required i32 code;
  2: optional string msg;
}

struct GetTimeReq {
}

struct GetTimeResp {
  1: required string time
  2: required Status status
}

service IService {
  GetTimeResp getTime(1: GetTimeReq req);
}
