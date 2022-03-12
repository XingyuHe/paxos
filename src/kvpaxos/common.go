package kvpaxos

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrNotAgreed = "ErrNotAgreed"
)
type Err string

type PutArgs struct {
  // You'll have to add definitions here.
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

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

type OrderedDict struct {
  stack []int64
  mapping map[int64]string
}

type KeyToPastPutIDToValue struct {
  mapping map[string]*OrderedDict
}

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