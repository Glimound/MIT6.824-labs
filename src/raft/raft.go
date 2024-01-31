package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// currentRole的枚举定义
const (
	follower int = iota
	candidate
	leader
)

// 超时期限（ms）
const (
	timeoutMin = 600
	timeoutMax = 800
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 2A
	currentTerm       int       // 当前（已知最新的）任期
	voted             bool      // 当前任期是否已投过通过票
	votedFor          int       // 当前任期投给的candidate的ID
	currentRole       int       // 当前角色（0-follower；1-candidate；2-leader）
	lastHeartbeatTime time.Time // 上次从leader接收到心跳的时间
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	// 注意此处加锁
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.currentRole == leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// 2A
	Term        int
	CandidateId int
	// TODO: 后续需补充
	//LastLogIndex int
	//LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// 2A
	rf.mu.Lock()
	// 处理请求投票逻辑
	// 若接收到的term比当前的大
	if args.Term > rf.currentTerm {
		DPrintf(dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, args.Term, rf.currentTerm)
		// 设置term并转换为follower
		rf.currentTerm = args.Term
		rf.currentRole = follower
		rf.lastHeartbeatTime = time.Now()
		rf.voted = false
	}
	// 若接收到的term比当前的小（不是小于等于）
	if args.Term < rf.currentTerm {
		// 投拒绝票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf(dVote, "S%d Deny voting to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		// 等于的情况（可能为转换而来的相等，或是原本就相等）
		// 对于term相等的情况的讨论：若当前为follower，不影响；
		// 对于candidate和leader，必然已经投给自己，会拒绝投票

		// 若该任期未投票，或已投过给该人（考虑网络请求重复），则投通过票，否则投拒绝票
		// TODO: 此处在后续需添加其余的逻辑判断，当前不涉及日志的处理逻辑
		if !rf.voted || rf.votedFor == args.CandidateId {
			// 投通过票
			DPrintf(dVote, "S%d Granting vote to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.voted = true
			rf.votedFor = args.CandidateId
			// 重置timeout counter
			rf.lastHeartbeatTime = time.Now()
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			DPrintf(dVote, "S%d Deny voting to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
		}
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 发送请求投票RPC并处理回复（Candidate）
func (rf *Raft) sendRequestVote(server int, c chan<- int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		// 处理接收投票逻辑
		// 若接收到的term比当前的大
		if reply.Term > rf.currentTerm {
			DPrintf(dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, reply.Term, rf.currentTerm)
			// 设置term并转换为follower
			rf.currentTerm = reply.Term
			rf.currentRole = follower
			rf.lastHeartbeatTime = time.Now()
			rf.voted = false
		}
		rf.mu.Unlock()
		// 若接收到有效投票
		if reply.VoteGranted {
			DPrintf(dTerm, "S%d <- S%d Got vote for T%d", rf.me, server, args.Term)
			// 向counterChan中发送信号
			c <- 1
		}
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//PrevLogIndex int
	//PrevLogTerm int
	//Entries[]
	//LeaderCommit
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// 处理追加条目逻辑
	// TODO: 此处在后续需添加其余的逻辑判断，当前只有心跳的处理逻辑

	// 若接收到的term比当前的大
	if args.Term > rf.currentTerm {
		// 设置term并转换为follower
		DPrintf(dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.currentRole = follower
		rf.voted = false
	}

	// 注意：收到来自term比自己小的leader的心跳，不要重置election timer，防止错误的leader持续工作
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf(dTimer, "S%d receive invalid AppEnt from S%d, T%d", rf.me, args.LeaderId, args.Term)
	} else {
		reply.Success = false
		DPrintf(dTimer, "S%d receive valid AppEnt from S%d, T%d", rf.me, args.LeaderId, args.Term)
		rf.lastHeartbeatTime = time.Now()
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// 发送AppendEntries RPC并处理回复（Leader）
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf(dLog1, "S%d -> S%d Sending AppendEntries RPC", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// TODO: 此处在后续需添加其余的逻辑判断，当前只有心跳的处理逻辑
	if ok {
		rf.mu.Lock()
		// 处理接收投票逻辑
		// 若接收到的term比当前的大
		if reply.Term > rf.currentTerm {
			// 设置term并转换为follower
			DPrintf(dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, reply.Term, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.currentRole = follower
			rf.lastHeartbeatTime = time.Now()
			rf.voted = false
		}
		rf.mu.Unlock()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 在范围内随机取一个timeout时间
		timeout := timeoutMin + rand.Int63n(timeoutMax-timeoutMin)
		// 循环直到超时
		for {
			// 检查当前是否被kill
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			// 超时且当前不为leader
			if time.Since(rf.lastHeartbeatTime).Milliseconds() > timeout && rf.currentRole != leader {
				// 此处本应有的unlock后移至函数末尾，以便在同一个lock中完成所有操作
				break
			}
			rf.mu.Unlock()
			time.Sleep(30 * time.Millisecond)
		}
		// 自增任期
		rf.currentTerm++
		// 给自己投票
		rf.currentRole = candidate
		rf.voted = true
		rf.votedFor = rf.me
		// 重置计时器
		rf.lastHeartbeatTime = time.Now()

		DPrintf(dTimer, "S%d Election timeout, become candidate for T%d", rf.me, rf.currentTerm)

		// 保存currentTerm的副本
		tmpTerm := rf.currentTerm

		// 创建用于goroutine间通信的channel；创建WaitGroup，以使所有操作完成后关闭channel
		counterChan := make(chan int, len(rf.peers))
		var wg sync.WaitGroup
		wg.Add(len(rf.peers) - 1) // 添加需要等待的goroutine数（除了自己外的peers数）

		// 创建goroutine判断票数是否过半，并处理转换为leader的相关操作
		go func(nodeNum int, c <-chan int, copyTerm int) {
			counter := 1
			// 读取counterChan中的内容直到关闭/跳出
			for i := range c {
				counter += i
				// 两整数相除，此处自动向下取整
				// 超过半数且任期未过期，则成为leader，并结束循环
				if counter > nodeNum/2 {
					rf.mu.Lock()
					// 若选举结果出来后还是当前任期
					if copyTerm == rf.currentTerm {
						// 转换为leader，且无需修改voted（任期没变）
						DPrintf(dLeader, "S%d Achieved majority for T%d (%d), converting to leader", rf.me, rf.currentTerm, counter)
						rf.currentRole = leader
						// 向所有其他节点定期发送空的AppendEntries RPC（心跳）
						// leader身份快速转变时（leader -> ? -> leader）会存在两个相同的heartbeat goroutine吗？不会。
						// leader能且只能因收到更大的term转换为follower，不可能在heartbeat timeout间完成候选与选举
						for j := range rf.peers {
							if j != rf.me {
								go rf.heartbeatTicker(j, copyTerm)
							}
						}
					}
					rf.mu.Unlock()
					// 票数已过半，无论是否有效，都应停止循环读取channel，结束该goroutine
					break
				}
			}
		}(len(rf.peers), counterChan, tmpTerm)

		// 向所有其他节点发送请求投票RPC
		DPrintf(dVote, "S%d Requesting vote", rf.me)
		for i := range rf.peers {
			if i != rf.me {
				go func(copyTerm int, c chan<- int, index int) {
					// 初始化args和reply
					args := RequestVoteArgs{}
					args.CandidateId = rf.me
					args.Term = copyTerm // 不能简单地使用rf.currentTerm，因为可能已经发生改变
					reply := RequestVoteReply{}
					rf.sendRequestVote(index, c, &args, &reply) // 此处不在加锁区内，无需担心call操作耗时

					// 发送并处理完投票后，令waitGroup-1
					wg.Done()
				}(tmpTerm, counterChan, i)
			}
		}
		rf.mu.Unlock()

		// 创建收尾goroutine，等待所有投票处理完后，关闭channel
		// 坑：若不创建goroutine，candidate在等待选举的情况下，其timeout时间会远超预期（等待waitgroup）
		go func() {
			wg.Wait()
			close(counterChan)
		}()
	}
}

// 向指定节点定期发送心跳
func (rf *Raft) heartbeatTicker(index int, copyTerm int) {
	// 坑：某个server被kill之后，ticker goroutine仍在运行，出现无限发送心跳等情况
	// 故还需检查是否已被kill
	for rf.killed() == false {
		// 若当前不再是leader，跳出循环
		rf.mu.Lock()
		if rf.currentRole != leader {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		// 初始化args和reply
		args := AppendEntriesArgs{}
		// TODO: 此处隐含一问题，rf.me会不会在运行过程中发生变化？此处未加锁
		args.LeaderId = rf.me
		args.Term = copyTerm // 不能简单地使用rf.currentTerm，因为可能已经发生改变
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(index, &args, &reply) // 此处使用goroutine，防止网络延迟导致心跳发送不规律
		time.Sleep(100 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// 2A
	rf.currentTerm = 0
	rf.voted = false
	rf.currentRole = follower
	rf.lastHeartbeatTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
