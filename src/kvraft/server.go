package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation int
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type Notification struct {
	err   Err
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store         map[string]string
	dupMap        map[int64]int64
	notifyChanMap map[int]chan Notification

	// 为什么不使用set存储id并判断是否重复？
	// 最后只会有一个server回应RPC，并在回应后将set中对应的entry删除
	// 此时其余的server没法在保证一致性的情况下判断哪些entry已废弃并删除，最终导致内存泄露
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Operation: OpGet,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d receive valid Get RPC from C%d, R%d", kv.me, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	kv.notifyChanMap[index] = notifyChan
	kv.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go kv.termDetector(term, index)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
		reply.Value = notification.value
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader // 超时，可能是leader变更了
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	if args.Op == "Put" {
		op.Operation = OpPut
	} else {
		op.Operation = OpAppend
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	DPrintf(dServer, "S%d receive valid %s RPC from C%d, R%d", kv.me, args.Op, args.ClientId, args.RequestId)

	notifyChan := make(chan Notification, 1)
	kv.notifyChanMap[index] = notifyChan
	kv.mu.Unlock()

	// when leader changed (term changed), it should redirect immediately
	go kv.termDetector(term, index)

	select {
	case notification := <-notifyChan:
		reply.Err = notification.err
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrWrongLeader // 超时，可能是leader变更了
	}

	kv.mu.Lock()
	delete(kv.notifyChanMap, index)
	kv.mu.Unlock()
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if !msg.CommandValid {
			continue
		}

		notification := Notification{}
		notification.err = OK
		op := msg.Command.(Op)

		kv.mu.Lock()
		lastRequestId, exist := kv.dupMap[op.ClientId]
		duplicate := exist && lastRequestId >= op.RequestId
		if !duplicate {
			switch op.Operation {
			case OpGet:
				if value, ok := kv.store[op.Key]; ok {
					notification.value = value
				} else {
					notification.err = ErrNoKey
				}
				DPrintf(dServer, "S%d done Get operation, log I%d, R%d, current key: %s, current value: %s",
					kv.me, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
			case OpPut:
				kv.store[op.Key] = op.Value
				DPrintf(dServer, "S%d done Put operation, log I%d, R%d, current key: %s, current value: %s",
					kv.me, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
			case OpAppend:
				kv.store[op.Key] = kv.store[op.Key] + op.Value
				DPrintf(dServer, "S%d done Append operation, log I%d, R%d, current key: %s, current value: %s",
					kv.me, msg.CommandIndex, op.RequestId, op.Key, kv.store[op.Key])
			}
			kv.dupMap[op.ClientId] = op.RequestId
		} else if op.Operation == OpGet {
			DPrintf(dServer, "S%d find duplicate log I%d, R%d <= %d, operation: %d", kv.me, msg.CommandIndex,
				op.RequestId, lastRequestId, op.Operation)
			if value, ok := kv.store[op.Key]; ok {
				notification.value = value
			} else {
				notification.err = ErrNoKey
			}
		} else {
			DPrintf(dServer, "S%d find duplicate log I%d, R%d <= %d, operation: %d", kv.me, msg.CommandIndex,
				op.RequestId, lastRequestId, op.Operation)
		}
		notifyChan, ok := kv.notifyChanMap[msg.CommandIndex]
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

func (kv *KVServer) termDetector(copyTerm int, index int) {
	for !kv.killed() {
		kv.mu.Lock()
		notifyChan, exist := kv.notifyChanMap[index]
		if !exist {
			kv.mu.Unlock()
			return
		}
		if currentTerm, _ := kv.rf.GetState(); currentTerm != copyTerm {
			DPrintf(dServer, "Detect term changed, current T%d != T%d", currentTerm, copyTerm)
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

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.dupMap = make(map[int64]int64)
	kv.notifyChanMap = make(map[int]chan Notification)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.applier()
	DPrintf(dServer, "Server S%d initiated", me)
	return kv
}
