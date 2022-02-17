package pbservice

import (
	"hash/fnv"
	"log"
)

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
	PutID int64

  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
	GetID int64
}


type GetReply struct {
  Err Err
  Value string
}


// Your RPC definitions here.
type ReplicateReply struct {
  Err Err
	Data map[string]string
}

type ReplicateArgs struct {
	Me string
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func (pa *PutArgs) Printf() {
	log.Printf("\t PutArgs: Key: %s, Value: %s, DoHash: %t", pa.Key, pa.Value, pa.DoHash)
}

