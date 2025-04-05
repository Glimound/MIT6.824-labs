package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation     int
	Key           string
	Value         string
	ClientId      int64
	RequestId     int64
	CommandId     int64
	Config        shardctrler.Config
	Store         map[string]string
	ShardsChanged []int
	DupMap        map[int64]int64
	ConfigNum     int
	Gid           int
}

type Notification struct {
	err   Err
	value string
}

type Snapshot struct {
	Store       map[string]string
	Index       int
	DupMap      map[int64]int64
	ShardStates [shardctrler.NShards]int
	Config      shardctrler.Config
	LastConfig  shardctrler.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead             int32
	store            map[string]string
	dupMap           map[int64]int64
	persister        *raft.Persister
	notifyChanMap    map[int64]chan Notification
	lastAppliedIndex int

	shardStates [shardctrler.NShards]int
	config      shardctrler.Config
	lastConfig  shardctrler.Config
	mck         *shardctrler.Clerk

	// use for duplicate detection
	clientId  int64
	requestId int64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid || kv.gid == 0 || kv.shardStates[shard] != Serving {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	op := Op{
		Operation: OpGet,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		CommandId: args.CommandId,
	}

	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d G%d receive valid Get RPC from C%d, R%d", kv.me, kv.gid, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	kv.notifyChanMap[op.CommandId] = notifyChan
	kv.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go kv.termDetector(term, op.CommandId)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
		reply.Value = notification.value
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader // 超时，可能是leader变更了
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, op.CommandId)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid || kv.gid == 0 || kv.shardStates[shard] != Serving {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		CommandId: args.CommandId,
	}
	if args.Op == "Put" {
		op.Operation = OpPut
	} else {
		op.Operation = OpAppend
	}

	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d G%d receive valid %s RPC from C%d, R%d", kv.me, kv.gid, args.Op, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	kv.notifyChanMap[op.CommandId] = notifyChan
	kv.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go kv.termDetector(term, op.CommandId)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader // 超时，可能是leader变更了
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, op.CommandId)
	kv.mu.Unlock()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			if msg.SnapshotValid {
				kv.installSnapshot(msg)
			}
			continue
		}

		notification := Notification{}
		notification.err = OK
		op := msg.Command.(Op)

		kv.mu.Lock()
		// 无需去重及回调的op
		switch op.Operation {
		case OpConfig:
			if op.Config.Num <= kv.config.Num {
				kv.mu.Unlock()
				continue
			}
			kv.lastConfig = kv.config
			kv.config = cloneConfig(op.Config)
			kv.updateShardsState()
			kv.lastAppliedIndex = msg.CommandIndex
			kv.snapshotTrigger()
			DPrintf(dServer, "S%d G%d applied config change from %d to %d, new shardStates: %v",
				kv.me, kv.gid, kv.lastConfig.Num, kv.config.Num, kv.shardStates)
			kv.mu.Unlock()
			continue
		}

		// 无需回调的op
		lastRequestId, exist := kv.dupMap[op.ClientId]
		duplicate := exist && lastRequestId >= op.RequestId
		switch op.Operation {
		case OpMerge:
			if duplicate {
				DPrintf(dServer, "S%d G%d ignoring duplicate OpMerge R%d <= %d",
					kv.me, kv.gid, op.RequestId, lastRequestId)
				kv.mu.Unlock()
				continue
			}
			if op.ConfigNum != kv.config.Num {
				DPrintf(dServer, "S%d G%d ignoring OpMerge with config %d, current: %d",
					kv.me, kv.gid, op.ConfigNum, kv.config.Num)
				kv.mu.Unlock()
				continue
			}
			fail := false
			for _, shard := range op.ShardsChanged {
				if kv.shardStates[shard] != Fetching {
					fail = true
					DPrintf(dError, "S%d G%d cannot apply OpMerge: shard %d not in Fetching state",
						kv.me, kv.gid, shard)
					break
				}
			}
			if fail {
				kv.mu.Unlock()
				continue
			}

			kv.store = mergeStore(op.Store, kv.store)
			kv.dupMap = mergeDupMap(op.DupMap, kv.dupMap)
			for _, shard := range op.ShardsChanged {
				kv.shardStates[shard] = Serving
			}
			if kv.isLeader() {
				go kv.fetchDoneSender(op.Gid, op.ConfigNum, op.ShardsChanged)
			}
			kv.dupMap[op.ClientId] = op.RequestId
			kv.lastAppliedIndex = msg.CommandIndex
			kv.snapshotTrigger()
			DPrintf(dServer, "S%d G%d applied OpMerge for shards %v in config %d, new shardStates: %v",
				kv.me, kv.gid, op.ShardsChanged, op.ConfigNum, kv.shardStates)
			kv.mu.Unlock()
			continue
		case OpClean:
			if duplicate {
				DPrintf(dServer, "S%d G%d ignoring duplicate OpClean R%d <= %d",
					kv.me, kv.gid, op.RequestId, lastRequestId)
				kv.mu.Unlock()
				continue
			}
			if op.ConfigNum != kv.config.Num {
				DPrintf(dServer, "S%d G%d ignoring OpClean with config %d, current: %d",
					kv.me, kv.gid, op.ConfigNum, kv.config.Num)
				kv.mu.Unlock()
				continue
			}
			fail := false
			for _, shard := range op.ShardsChanged {
				if kv.shardStates[shard] != Pushing {
					fail = true
					DPrintf(dError, "S%d G%d cannot apply OpClean: shard %d not in Pushing state",
						kv.me, kv.gid, shard)
					break
				}
			}
			if fail {
				kv.mu.Unlock()
				continue
			}

			kv.store = excludeStore(kv.store, op.ShardsChanged)
			for _, shard := range op.ShardsChanged {
				kv.shardStates[shard] = Serving
			}
			kv.dupMap[op.ClientId] = op.RequestId
			kv.lastAppliedIndex = msg.CommandIndex
			kv.snapshotTrigger()
			DPrintf(dServer, "S%d G%d applied OpClean for shards %v in config %d, new shardStates: %v",
				kv.me, kv.gid, op.ShardsChanged, op.ConfigNum, kv.shardStates)
			kv.mu.Unlock()
			continue
		}

		// 需要去重及回调的op
		if !duplicate {
			switch op.Operation {
			case OpGet:
				if value, ok := kv.store[op.Key]; ok {
					notification.value = value
				} else {
					notification.err = ErrNoKey
				}
				//DPrintf(dServer, "S%d G%d done Get operation, log I%d, R%d, current key: %s, current value: %s",
				//	kv.me, kv.gid, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
				DPrintf(dServer, "S%d G%d done Get operation, log I%d, R%d", kv.me, kv.gid, msg.CommandIndex, op.RequestId)
			case OpPut:
				kv.store[op.Key] = op.Value
				//DPrintf(dServer, "S%d G%d done Put operation, log I%d, R%d, current key: %s, current value: %s",
				//	kv.me, kv.gid, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
				DPrintf(dServer, "S%d G%d done Put operation, log I%d, R%d", kv.me, kv.gid, msg.CommandIndex, op.RequestId)
			case OpAppend:
				kv.store[op.Key] = kv.store[op.Key] + op.Value
				//DPrintf(dServer, "S%d G%d done Append operation, log I%d, R%d, current key: %s, current value: %s",
				//	kv.me, kv.gid, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
				DPrintf(dServer, "S%d G%d done Append operation, log I%d, R%d", kv.me, kv.gid, msg.CommandIndex, op.RequestId)
			}
			kv.dupMap[op.ClientId] = op.RequestId
			kv.lastAppliedIndex = msg.CommandIndex
			kv.snapshotTrigger()
		} else if op.Operation == OpGet {
			DPrintf(dServer, "S%d G%d find duplicate log I%d, R%d <= %d, operation: %d", kv.me, kv.gid, msg.CommandIndex,
				op.RequestId, lastRequestId, op.Operation)
			if value, ok := kv.store[op.Key]; ok {
				notification.value = value
			} else {
				notification.err = ErrNoKey
			}
		} else {
			DPrintf(dServer, "S%d G%d find duplicate log I%d, R%d <= %d, operation: %d", kv.me, kv.gid, msg.CommandIndex,
				op.RequestId, lastRequestId, op.Operation)
		}
		notifyChan, ok := kv.notifyChanMap[op.CommandId]
		if ok {
			// 必须使用非阻塞发送，因为RPC处理器可能已经超时
			select {
			case notifyChan <- notification:
			default:
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) configDetector() {
updateConfig:
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		if !kv.isLeader() {
			continue
		}
		kv.mu.Lock()
		for _, state := range kv.shardStates {
			if state != Serving {
				kv.mu.Unlock()
				continue updateConfig
			}
		}
		num := kv.config.Num + 1
		kv.mu.Unlock()

		// 在server崩溃重启重放raft日志时，可能插入顺序错误的config
		newConfig := kv.mck.Query(num)
		if newConfig.Num == num {
			kv.rf.Start(Op{
				Operation: OpConfig,
				Config:    cloneConfig(newConfig),
			})
		}
	}
}

// shards的状态流转handler（serving, fetching, pushing）
func (kv *ShardKV) updateShardsState() {
	shardsToFetch := make(map[int][]int) // gid -> shardIndex

	for i := 0; i < shardctrler.NShards; i++ {
		oldGid := kv.lastConfig.Shards[i]
		newGid := kv.config.Shards[i]
		if oldGid == kv.gid && newGid != kv.gid {
			kv.shardStates[i] = Pushing
		}
		if oldGid != 0 && oldGid != kv.gid && newGid == kv.gid {
			kv.shardStates[i] = Fetching
			targetGid := kv.lastConfig.Shards[i]
			shardsToFetch[targetGid] = append(shardsToFetch[targetGid], i)
		}
	}
	if len(shardsToFetch) != 0 && kv.isLeader() {
		DPrintf(dServer, "S%d G%d needs to fetch shards for config %d, shardsMap: %v",
			kv.me, kv.gid, kv.config.Num, shardsToFetch)
		go kv.fetchShardsSender(shardsToFetch, kv.config.Num)
	}
}

func (kv *ShardKV) fetchShardsSender(shardsToFetch map[int][]int, configNum int) {
	for gid, shards := range shardsToFetch {
		kv.mu.Lock()
		reqId := kv.requestId
		kv.requestId++
		clientId := kv.clientId
		kv.mu.Unlock()

		fetchArgs := FetchShardsArgs{
			ConfigNum:  configNum,
			ShardIndex: clone(shards),
			ClientId:   clientId,
			RequestId:  reqId,
		}
		fetchReply := FetchShardsReply{}
		kv.sendFetchShards(gid, &fetchArgs, &fetchReply)
	}
}

func (kv *ShardKV) sendFetchShards(gid int, fetchArgs *FetchShardsArgs, fetchReply *FetchShardsReply) {
	kv.mu.Lock()
	servers := kv.lastConfig.Groups[gid]
	kv.mu.Unlock()
	for !kv.killed() {
		for _, server := range servers {
			DPrintf(dServer, "S%d G%d sending fetch shards RPC to S%s", kv.me, kv.gid, server)
			ok := kv.make_end(server).Call("ShardKV.FetchShards", fetchArgs, fetchReply)
			if ok && fetchReply.Err == OK {
				if fetchReply.ConfigNum < fetchArgs.ConfigNum {
					// target server is not prepared, retry until it is
					DPrintf(dServer, "S%d G%d target S%s not ready for config %d, current: %d",
						kv.me, kv.gid, server, fetchArgs.ConfigNum, fetchReply.ConfigNum)
					continue
				}
				if fetchReply.ConfigNum > fetchArgs.ConfigNum {
					// current request is outdated, return
					DPrintf(dServer, "S%d G%d fetch request outdated, requested: %d, current: %d",
						kv.me, kv.gid, fetchArgs.ConfigNum, fetchReply.ConfigNum)
					return
				}

				DPrintf(dServer, "S%d G%d successfully fetched shards %v from gid %d for config %d",
					kv.me, kv.gid, fetchArgs.ShardIndex, gid, fetchArgs.ConfigNum)

				// requestId对于同一次fetch，但向不同server发送的请求，应当不同；否则均为相同requestId，第二次OpMerge会被忽略
				kv.rf.Start(Op{
					Operation:     OpMerge,
					Store:         cloneMap(fetchReply.Store),
					ShardsChanged: clone(fetchReply.ShardsChanged),
					DupMap:        cloneDupMap(fetchReply.DupMap),
					ConfigNum:     fetchReply.ConfigNum,
					Gid:           gid,
					ClientId:      fetchArgs.ClientId,
					RequestId:     fetchArgs.RequestId,
				})
				return
			}
			if ok && fetchReply.Err == ErrWrongGroup {
				DPrintf(dError, "S%d G%d sending fetch shards to wrong group", kv.me)
				panic("sending fetch shards to wrong group")
			}
			if ok && fetchReply.Err == ErrStaleRequest {
				DPrintf(dServer, "S%d G%d fetch request outdated", kv.me, kv.gid)
				return
			}
			// wrong leader or not ok: retry
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) FetchShards(fetchArgs *FetchShardsArgs, fetchReply *FetchShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		fetchReply.Err = ErrWrongLeader
		return
	}

	fetchReply.ConfigNum = kv.config.Num
	if fetchArgs.ConfigNum != kv.config.Num {
		fetchReply.Err = OK
		DPrintf(dServer, "S%d G%d received FetchShards RPC with config %d, but current config is %d",
			kv.me, kv.gid, fetchArgs.ConfigNum, kv.config.Num)
		return
	}

	fail := false
	for _, shard := range fetchArgs.ShardIndex {
		if kv.shardStates[shard] != Pushing {
			fail = true
			DPrintf(dServer, "S%d G%d cannot send shard %d: not in Pushing state (state=%d)",
				kv.me, kv.gid, shard, kv.shardStates[shard])
			break
		}
	}
	if fail {
		fetchReply.Err = ErrStaleRequest
		return
	}

	DPrintf(dServer, "S%d G%d sending shards %v for config %d", kv.me, kv.gid, fetchArgs.ShardIndex, fetchArgs.ConfigNum)

	fetchReply.ShardsChanged = fetchArgs.ShardIndex
	fetchReply.Store = extractStore(kv.store, fetchArgs.ShardIndex)
	fetchReply.DupMap = kv.dupMap
	fetchReply.Err = OK
}

func (kv *ShardKV) fetchDoneSender(gid int, configNum int, shardsChanged []int) {
	kv.mu.Lock()
	reqId := kv.requestId
	kv.requestId++
	clientId := kv.clientId
	kv.mu.Unlock()
	doneArgs := FetchDoneArgs{
		ConfigNum:  configNum,
		ShardIndex: clone(shardsChanged),
		ClientId:   clientId,
		RequestId:  reqId,
	}
	doneReply := FetchDoneReply{}
	kv.sendFetchDone(gid, &doneArgs, &doneReply)
}

func (kv *ShardKV) sendFetchDone(gid int, doneArgs *FetchDoneArgs, doneReply *FetchDoneReply) {
	kv.mu.Lock()
	servers := kv.lastConfig.Groups[gid]
	kv.mu.Unlock()
	for !kv.killed() {
		for _, server := range servers {
			ok := kv.make_end(server).Call("ShardKV.FetchDone", doneArgs, doneReply)
			if ok && doneReply.Err == OK {
				DPrintf(dServer, "S%d G%d successfully sent FetchDone to S%s for shards %v, config %d",
					kv.me, kv.gid, server, doneArgs.ShardIndex, doneArgs.ConfigNum)
				return
			}
			if ok && doneReply.Err == ErrStaleRequest {
				DPrintf(dServer, "S%d G%d fetch done request outdated", kv.me, kv.gid)
				return
			}
			// wrong leader or not ok: retry
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) FetchDone(doneArgs *FetchDoneArgs, doneReply *FetchDoneReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isLeader() {
		doneReply.Err = ErrWrongLeader
		return
	}

	if doneArgs.ConfigNum != kv.config.Num {
		doneReply.Err = OK
		DPrintf(dServer, "S%d G%d received FetchDone RPC with config %d, but current config is %d",
			kv.me, kv.gid, doneArgs.ConfigNum, kv.config.Num)
		return
	}

	fail := false
	for _, shard := range doneArgs.ShardIndex {
		if kv.shardStates[shard] != Pushing {
			fail = true
			DPrintf(dServer, "S%d G%d cannot clean shard %d: not in Pushing state (state=%d)",
				kv.me, kv.gid, shard, kv.shardStates[shard])
			break
		}
	}
	if fail {
		doneReply.Err = ErrStaleRequest
		return
	}

	DPrintf(dServer, "S%d G%d received FetchDone for shards %v, config %d",
		kv.me, kv.gid, doneArgs.ShardIndex, doneArgs.ConfigNum)

	kv.rf.Start(Op{
		Operation:     OpClean,
		ConfigNum:     doneArgs.ConfigNum,
		ShardsChanged: clone(doneArgs.ShardIndex),
		ClientId:      doneArgs.ClientId,
		RequestId:     doneArgs.RequestId,
	})
	doneReply.Err = OK
}

func extractStore(src map[string]string, shardsChanged []int) map[string]string {
	dest := make(map[string]string)
	shardsSet := make(map[int]bool)
	for _, shard := range shardsChanged {
		shardsSet[shard] = true
	}
	for k, v := range src {
		if shardsSet[key2shard(k)] {
			dest[k] = v
		}
	}
	return dest
}

func excludeStore(src map[string]string, shardsChanged []int) map[string]string {
	shardsSet := make(map[int]bool)
	for _, shard := range shardsChanged {
		shardsSet[shard] = true
	}
	for k := range src {
		if shardsSet[key2shard(k)] {
			delete(src, k)
		}
	}
	return src
}

func mergeStore(src map[string]string, dest map[string]string) map[string]string {
	for k, v := range src {
		dest[k] = v
	}
	return dest
}

func mergeDupMap(src map[int64]int64, dest map[int64]int64) map[int64]int64 {
	for k, v := range src {
		destV, exists := dest[k]
		if !exists {
			dest[k] = v
		} else if v > destV {
			dest[k] = v
		}
		// 如果v <= destV，保持dest[k]不变
	}
	return dest
}

func cloneMap(src map[string]string) map[string]string {
	dest := make(map[string]string)
	for k, v := range src {
		dest[k] = v
	}
	return dest
}

func cloneDupMap(src map[int64]int64) map[int64]int64 {
	dest := make(map[int64]int64)
	for k, v := range src {
		dest[k] = v
	}
	return dest
}

func cloneConfig(oldConfig shardctrler.Config) shardctrler.Config {
	newConfig := shardctrler.Config{}
	newConfig.Num = oldConfig.Num
	newConfig.Shards = oldConfig.Shards
	newConfig.Groups = make(map[int][]string)
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = make([]string, len(v))
		copy(newConfig.Groups[k], v)
	}
	return newConfig
}

func clone(old []int) []int {
	new := make([]int, len(old))
	copy(new, old)
	return new
}

func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

func (kv *ShardKV) installSnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ok := kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot)
	if ok {
		s := bytes.NewBuffer(msg.Snapshot)
		sd := labgob.NewDecoder(s)
		var snapshot Snapshot
		sd.Decode(&snapshot)
		kv.store = snapshot.Store
		kv.lastAppliedIndex = snapshot.Index
		kv.dupMap = snapshot.DupMap
		kv.shardStates = snapshot.ShardStates
		kv.config = snapshot.Config
		kv.lastConfig = snapshot.LastConfig
		DPrintf(dServer, "S%d G%d installed snapshot, snapshot to I%d", kv.me, kv.gid, kv.lastAppliedIndex)
	}
}

func (kv *ShardKV) termDetector(copyTerm int, commandId int64) {
	for !kv.killed() {
		kv.mu.Lock()
		notifyChan, exist := kv.notifyChanMap[commandId]
		if !exist {
			kv.mu.Unlock()
			return
		}
		if currentTerm, _ := kv.rf.GetState(); currentTerm != copyTerm {
			DPrintf(dServer, "S%d G%d Detect term changed, current T%d != T%d",
				kv.me, kv.gid, currentTerm, copyTerm)
			notification := Notification{}
			notification.err = ErrWrongLeader
			select {
			case notifyChan <- notification:
			default:
			}
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// must be called within lock area
func (kv *ShardKV) snapshotTrigger() {
	if kv.killed() || kv.maxraftstate < 0 {
		return
	}

	if kv.persister.RaftStateSize() <= kv.maxraftstate {
		return
	}

	DPrintf(dServer, "S%d G%d Reach snapshot threshold, snapshot to I%d", kv.me, kv.gid, kv.lastAppliedIndex)

	snapshot := Snapshot{
		Store:       kv.store,
		Index:       kv.lastAppliedIndex,
		DupMap:      kv.dupMap,
		ShardStates: kv.shardStates,
		Config:      kv.config,
		LastConfig:  kv.lastConfig,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshot)
	snapshotBytes := w.Bytes()

	kv.rf.Snapshot(kv.lastAppliedIndex, snapshotBytes)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.clientId = nrand()
	kv.requestId = 0

	kv.store = make(map[string]string)
	kv.dupMap = make(map[int64]int64)
	kv.notifyChanMap = make(map[int64]chan Notification)
	kv.persister = persister
	kv.lastAppliedIndex = 0
	if snapshotBytes := persister.ReadSnapshot(); len(snapshotBytes) != 0 {
		s := bytes.NewBuffer(snapshotBytes)
		sd := labgob.NewDecoder(s)
		var snapshot Snapshot
		sd.Decode(&snapshot)
		kv.store = snapshot.Store
		kv.lastAppliedIndex = snapshot.Index
		kv.dupMap = snapshot.DupMap
		kv.shardStates = snapshot.ShardStates
		kv.config = snapshot.Config
		kv.lastConfig = snapshot.LastConfig
	}

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetDebugType("shardkv")

	go kv.applier()
	go kv.configDetector()
	DPrintf(dServer, "SKV Server S%d G%d initiated", me, gid)
	return kv
}
