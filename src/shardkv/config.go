package shardkv

import (
	"shardmaster"
)

// This file contains all functions related to reconfigurations
func (kv *ShardKV) sendOpPaxosRemoteAll(newConfig *shardmaster.Config) {

	for _, oldServers := range kv.config.Groups {
		kv.sendOpPaxosRemote(oldServers)
	}
	for _, newServers := range newConfig.Groups {
		kv.sendOpPaxosRemote(newServers)
	}
}


func (kv *ShardKV) sendOpPaxosRemote(servers []string) {
}

// RPC
func (kv *ShardKV) MoveShards(args *MoveShardsArgs, reply *MoveShardsReply) error {

	kv.mu.Lock(); defer kv.mu.Unlock()

	DB := makeDebugger("MoveShards", args.ID, kv.me, kv.gid)
	DB.printf(1, "MoveArgs: ", args)
	DB.printf(2, "Incoming Num: ", args.Num, "Local Num: ", kv.config.Num)

	if (args.Num - kv.config.Num > 1) {
		DB.printf(3, "exiting")
		go kv.tick()
		return ErrSourceServerBehind
	}

	candidateOp := kv.buildPaxosMoveShardsOp(args)
	paxosOK := kv.sendOpPaxosLcl(candidateOp)

	if paxosOK {
		*reply = kv.opToReply(&candidateOp).(MoveShardsReply)
		DB.printf(2, "paxosOK!, reply: ", reply)
		return nil
	} else {
		return ErrPaxosFailed
	}
}


// shard related ops
func (kv *ShardKV) isKeyInShard(key string) bool {
	DB := makeDebugger("isKeyInShard", 0, kv.me, kv.gid)
	DB.printf(1, "key: ", key, "shard: ", key2shard(key), " tgtGID: ", kv.config.Shards[key2shard(key)], " Num: ", kv.config.Num)
	// DB.printf(2, "config: ", kv.config.ToString())
	if kv.config.Num == 0 {
		return false
	}
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) findNewShards(oldConfig *shardmaster.Config, newConfig *shardmaster.Config) []int {
	DB := makeDebugger("findNewShards", 0, kv.me, kv.gid)
	DB.printf(1, "oldConfig: ", oldConfig.ToString())
	DB.printf(1, "newConfig: ", newConfig.ToString())

	oldShards := makeSet()
	newShards := makeSet()

	for shard, gid := range oldConfig.Shards {
		if gid == kv.gid {
			oldShards.insert(shard)
		}
	}
	DB.printf(2, "oldShards: ", oldShards)

	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			newShards.insert(shard)
		}
	}
	DB.printf(2, "newShards: ", newShards)
	DB.printf(3, "kv.gid: ", kv.gid)
	DB.printf(4, "newShard - oldShard: ", newShards.minus(&oldShards).toArray())


	return newShards.minus(&oldShards).toArray()
}

// set stuff
func makeSet() Set{
	return Set{elements: make(map[int]bool)}
}

func (st *Set) insert(ele int) {
	st.elements[ele] = true;
}

func (st *Set) contain(ele int) bool {
	ok, _ := st.elements[ele]
	return ok
}

func (st *Set) minus(st2 *Set) Set {
	ret := makeSet()

	for ele, _ := range st.elements {
		if !st2.contain(ele) {
			ret.insert(ele)
		}
	}
	return ret
}

func (st Set) toArray() [] int {
	var ret [] int
	for ele, _ := range st.elements {
		ret = append(ret, ele)
	}
	return ret
}