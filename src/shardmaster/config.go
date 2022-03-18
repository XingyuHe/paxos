package shardmaster

func (sm *ShardMaster) initialized() bool {
	return len(sm.configs[len(sm.configs) - 1].Groups) != 0
}

func (sm *ShardMaster) makeConfig(Num int, Shards [NShards]int64, Groups map[int64][]string) Config {
	var ret Config
	ret.Num = Num
	ret.Shards = Shards
	ret.Groups = Groups
	return ret
}

type GIDToShard struct {
	gidToShard map[int64][]int
	szToGIDs [NShards + 1][]int64
	maxSz int
	minSz int
}

func (gts *GIDToShard) PushMinGrp(shard int, leaveGID int64) int64{

	DB := makeDebugger("PushMinGrp", 0, 0)
	DB.printf(1, "shard to add: ", shard)

	var pushGIDs []int64
	var pushGID int64

	for {
		DB.printf(2, *gts)
		if gts.minSz >= len(gts.szToGIDs) {
			return 0
		}

		pushGIDs = gts.szToGIDs[gts.minSz]
		pushGID = pushGIDs[len(pushGIDs) - 1]

		// clean up
		gts.szToGIDs[gts.minSz] = gts.szToGIDs[gts.minSz][:len(pushGIDs) - 1]
		if pushGID != leaveGID {
			gts.szToGIDs[gts.minSz + 1] = append(gts.szToGIDs[gts.minSz+1], pushGID)
			for len(gts.szToGIDs[gts.minSz]) == 0 {
				gts.minSz++
			}
			break
		}

		for gts.minSz < len(gts.szToGIDs) && len(gts.szToGIDs[gts.minSz]) == 0 {
			gts.minSz++
		}

	}

	DB.printf(3, *gts)
	DB.printf(4, "pushGID: ", pushGID)

	return pushGID
}

func (gts *GIDToShard) PopExtraShard() int {

	if gts.maxSz == 0 {
		return -1
	}

	popGIDs := gts.szToGIDs[gts.maxSz]
	popGID := popGIDs[len(popGIDs) - 1]

	popShards := gts.gidToShard[popGID]
	popShard := popShards[len(popShards) - 1]

	// clean up
	gts.szToGIDs[gts.maxSz] = gts.szToGIDs[gts.maxSz][:len(popGIDs) - 1]
	if gts.maxSz - 1 >= 0 {
		gts.szToGIDs[gts.maxSz - 1] = append(gts.szToGIDs[gts.maxSz-1], popGID)
	}
	if len(gts.szToGIDs[gts.maxSz]) == 0 {
		gts.maxSz--
	}

	gts.gidToShard[popGID] = gts.gidToShard[popGID][:len(popShards) - 1]
	gts.minSz = Min(gts.minSz, gts.maxSz - 1)

	return popShard
}


func (cf *Config) buildGIDToShard() GIDToShard {

	DB := makeDebugger("buildGIDToShard", 0, 0)
	DB.printf(1)

	gidToShardDict := make(map[int64][]int)
	for shardIdx, gid := range cf.Shards {
		gidToShardDict[gid] = append(gidToShardDict[gid], shardIdx)
	}
	for gid, _ := range cf.Groups {
		if _, ok := gidToShardDict[gid]; !ok {
			gidToShardDict[gid] = make([]int, 0)
		}
	}

	var ret GIDToShard
	ret.gidToShard = gidToShardDict
	ret.minSz = NShards

	for gid, shards := range ret.gidToShard {
		ret.szToGIDs[len(shards)] = append(ret.szToGIDs[len(shards)], gid)
		ret.maxSz = Max(ret.maxSz, len(shards))
		ret.minSz = Min(ret.minSz, len(shards))
	}

	DB.printf(2, "GIDToShard: ", ret)

	return ret
}

func (sm *ShardMaster) joinNewShardFromOld(args *JoinArgs, oldConfig *Config) [NShards]int64{
	DB := makeDebugger("joinNewShardFromOld", 0, sm.me)
	DB.printf(1, "oldConfig: ", oldConfig)

	var shardToGID [NShards]int64

	for shard, gid := range oldConfig.Shards {
		shardToGID[shard] = gid
	}

	gidToShard := oldConfig.buildGIDToShard()
	shardCntToNewGrp := 0
	DB.printf(1, "gidToShard: ", gidToShard)

	for gidToShard.maxSz - shardCntToNewGrp > 1 {
		incomeShard := gidToShard.PopExtraShard()
		if (incomeShard < 0) {
			return shardToGID
		}
		shardToGID[incomeShard] = args.GID
		shardCntToNewGrp++
	}
	DB.printf(2, "shardToGID: ", shardToGID)

	return shardToGID
}

func (sm *ShardMaster) leaveNewShardFromOld(args *LeaveArgs, oldConfig *Config) [NShards]int64{
	DB := makeDebugger("leaveNewShardFromOld", 0, sm.me)
	DB.printf(1, "oldConfig: ", oldConfig)
	var shardToGID [NShards]int64

	for shard, gid := range oldConfig.Shards {
		shardToGID[shard] = gid
	}

	gidToShard := oldConfig.buildGIDToShard()
	shardsToAdd := gidToShard.gidToShard[args.GID]
	DB.printf(2, "shardsToAdd: ", shardsToAdd)

	for _, shard := range shardsToAdd {
		pushGID := gidToShard.PushMinGrp(shard, args.GID)
		shardToGID[shard] = pushGID
	}

	DB.printf(3, "shardToGID: ", shardToGID)

	return shardToGID
}