package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "fmt"

type Clerk struct {
  mu sync.Mutex // one RPC at a time
  sm *shardmaster.Clerk
  config shardmaster.Config
  // You'll have to modify Clerk.
}



func MakeClerk(shardmasters []string) *Clerk {
  ck := new(Clerk)
  ck.sm = shardmaster.MakeClerk(shardmasters)
  // You'll have to modify MakeClerk.
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
  shard := 0
  if len(key) > 0 {
    shard = int(key[0])
  }
  shard %= shardmaster.NShards
  return shard
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  // You'll have to modify Get().
  DB := makeDebugger("GetExt", 0, 0, 0)
  id := genID()

  for {
    shard := key2shard(key)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &GetArgs{}
        args.Key = key
        args.ID = id
        DB.printf(1, "GetArgs: ", args.toString())
        var reply GetReply
        ok := call(srv, "ShardKV.Get", args, &reply)
        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
          return reply.Value
        }
        if ok && (reply.Err == ErrWrongGroup) {
          break
        }
        DB.printf(1, ok)
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()

  // You'll have to modify Put().
  DB := makeDebugger("PutExt", 0, 0, 0)
  id := genID()

  for {
    shard := key2shard(key)

    gid := ck.config.Shards[shard]

    servers, ok := ck.config.Groups[gid]


    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &PutArgs{}
        args.Key = key
        args.Value = value
        args.DoHash = dohash
        args.ID = id
        DB.printf(1, "PutArgs: ", args.toString())
        var reply PutReply
        ok := call(srv, "ShardKV.Put", args, &reply)
        if ok && reply.Err == OK {
          DB.printf(2, "everything good")
          return reply.PreviousValue
        }
        if ok && (reply.Err == ErrWrongGroup) {
          DB.printf(3, "ErrWrongGroup")
          break
        }
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
