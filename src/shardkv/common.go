package shardkv

import (
	"errors"
	"hash/fnv"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)
var ErrPaxosFailed = errors.New("ErrPaxosFailed")

type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  ID int64
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  ID int64
}

type GetReply struct {
  Err Err
  Value string
}

type MoveShardsArgs struct {
  Num int
  ID int64
  Shards []int
}

type MoveShardsReply struct {
  Err Err
  KPV map[string]OrderedDict
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}


// for kv server
type OrderedDict struct {
  Stack []int64
  Mapping map[int64]string
}

type KeyToPastPutIDToValue struct {
  mapping map[string]*OrderedDict
}

// agree
type PutAgree struct {
  PutID int64
  Key string
  Val string
  DoHash bool
}

type GetAgree struct {
  Key string
  GetID int64
  LastPutID int64
}

type ConfigAgree struct {
  ID int64
  Num int
}

type MoveShardAgree struct {
  ID int64
  Num int
  Shards []int
}


type Set struct {
  elements map[int]bool
}

