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
	"fmt"
	"math/rand"
	"strings"
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

	// 2B
	log         []LogEntry    // 日志（index从1开始）
	commitIndex int           // 已commit的日志条目的最高index（单调递增）
	lastApplied int           // 已apply至状态机的日志条目的最高index（单调递增）
	nextIndex   []int         // 每个节点的下一个日志复制的位置index（仅leader 易失）
	matchIndex  []int         // 每个节点的最高已复制日志的index（仅leader 易失 单调递增）
	applyCh     chan ApplyMsg // 与client通信的管道
	cond        *sync.Cond    // 用于控制当前是否该开启log replicate的条件变量
	logSending  bool          // 指示当前是否有log replicate正在执行
}

type LogEntry struct {
	Command interface{}
	Term    int
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

	// 2B
	LastLogIndex int
	LastLogTerm  int
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
		rf.voted = false
	}
	// 坑: term比自己大但投了拒绝票，不应该重置election timeout，否则可能会导致日志陈旧的节点阻止日志较新节点开启选举
	// 若恰好日志陈旧的节点的超时时间比其他节点超时时间都短，则会陷入死循环
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

		// 若该任期未投票，或已投过给该人（考虑网络请求重复），则下一步判断，否则投拒绝票
		if !rf.voted || rf.votedFor == args.CandidateId {
			if args.LastLogTerm > rf.log[len(rf.log)-1].Term {
				// 投通过票
				DPrintf(dVote, "S%d Granting vote to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				rf.voted = true
				rf.votedFor = args.CandidateId
				// 重置timeout counter
				rf.lastHeartbeatTime = time.Now()
			} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
				if args.LastLogIndex >= len(rf.log)-1 {
					// 投通过票
					DPrintf(dVote, "S%d Granting vote to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
					reply.Term = rf.currentTerm
					reply.VoteGranted = true
					rf.voted = true
					rf.votedFor = args.CandidateId
					// 重置timeout counter
					rf.lastHeartbeatTime = time.Now()
				} else {
					// 投拒绝票
					reply.Term = rf.currentTerm
					reply.VoteGranted = false
					DPrintf(dVote, "S%d Deny voting to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
				}
			} else {
				// 投拒绝票
				reply.Term = rf.currentTerm
				reply.VoteGranted = false
				DPrintf(dVote, "S%d Deny voting to S%d at T%d", rf.me, args.CandidateId, rf.currentTerm)
			}
		} else {
			// 投拒绝票
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
	// 2A
	Term     int
	LeaderId int

	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// 处理追加条目逻辑

	// 若接收到的term比当前的大（对于candidate，则是大于等于）
	if args.Term > rf.currentTerm {
		// 设置term并转换为follower
		DPrintf(dTerm, "S%d Find bigger term (%d>%d), converting to follower", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.currentRole = follower
		rf.voted = false
	} else if rf.currentRole == candidate && args.Term == rf.currentTerm {
		// 转换为follower
		DPrintf(dVote, "S%d Find existing leader, converting to follower", rf.me)
		rf.currentRole = follower
	}

	// 注意：收到来自term比自己小的leader的心跳，不要重置election timer，防止错误的leader持续工作
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf(dTimer, "S%d receive invalid AppEnt from S%d, T%d", rf.me, args.LeaderId, args.Term)
	} else if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// index大于当前日志长度或条目不匹配，则进入该分支，返回false，等待leader重新发送
		// log为空时：index和term均为0，此时必定成功，不会进入该分支
		reply.Success = false
		DPrintf(dTimer, "S%d receive mismatched AppEnt from S%d, T%d", rf.me, args.LeaderId, args.Term)
		rf.lastHeartbeatTime = time.Now()
	} else {
		reply.Success = true
		DPrintf(dTimer, "S%d receive valid AppEnt from S%d, T%d", rf.me, args.LeaderId, args.Term)
		rf.lastHeartbeatTime = time.Now()
		// 无论是否有entries（是否是心跳），都进入该循环，判断并复制日志
		for i := range args.Entries {
			// 依次判断是否重复，若重复则删除后面的所有log，并开始append
			// 判断是否长度超过rf.log（即后面没有其它entries）
			if args.PrevLogIndex+1+i > len(rf.log)-1 {
				// 若log后面无其它entries，直接append剩余的entries
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			// 若日志有冲突
			if rf.log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
				// 删除后面所有的log，并直接append剩余entries
				rf.log = rf.log[:args.PrevLogIndex+1+i]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
			// 否则往后遍历，直到找到冲突点或空槽
		}
		if len(args.Entries) != 0 {
			DPrintf(dLog2, "S%d new log: %s", rf.me, EntriesToString(rf.log))
		}
	}

	// 若未失败，根据leader的commitIndex来更新自己的commitIndex
	if reply.Success && args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
		DPrintf(dLog2, "S%d change commitIndex to I%d", rf.me, rf.commitIndex)
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// 发送AppendEntries RPC并处理回复（Leader）
// 返回值：ok, success
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (bool, bool) {
	DPrintf(dLog1, "S%d -> S%d Sending AppEnt PLI:%d PLT:%d LC:%d - [%s]",
		rf.me, server, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, EntriesToString(args.Entries))
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	success := false
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

		if reply.Success {
			success = true
		}
		rf.mu.Unlock()
	}
	return ok, success
}

func EntriesToString(entries []LogEntry) string {
	var parts []string
	for _, entry := range entries {
		parts = append(parts, fmt.Sprintf("(%v, T%d)", entry.Command, entry.Term))
	}
	return strings.Join(parts, ",")
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

	// Your code here (2B).

	rf.mu.Lock()
	// 下一个放置log entry的位置（index从1开始）
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.currentRole == leader

	if isLeader {
		DPrintf(dLog1, "S%d receive new command (%v) at, I%d", rf.me, command, index)
		entry := LogEntry{command, term}
		rf.log = append(rf.log, entry)
		DPrintf(dLog2, "S%d new log: %s", rf.me, EntriesToString(rf.log))
		rf.cond.Broadcast()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// 用于发送日志给follower
// 合并部分log，不会再每收到一次command就调用一次完整的日志复制操作
// 防止收到并发的command时，有冗余的sendLogEntries运行的情况
func (rf *Raft) logSender() {
	for {
		rf.mu.Lock()
		// 开始复制日志的三个条件：当前是leader，有新条目可以发送，没有正在发送的条目
		// 任一条件不满足则会等待
		// 坑：logSending不会转为false：counterChan接收的投票因部分node killed所以始终达不到半数
		for rf.currentRole != leader || len(rf.log)-1 <= rf.commitIndex || rf.logSending {
			rf.cond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		rf.logSending = true
		// 为logSending设置timeout，若超时且当前复制过程仍未结束，则置为false
		// 此处是为了防止网络请求的时间长，不可避免会有长时间等待，导致logSending缓慢改变->下一次日志复制等待很久
		go func(copyCommitIndex int) {
			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			// 此时commit可能已经前进（在commit过半时已经置为false，开始了下一轮复制），则不再置false
			if copyCommitIndex == rf.commitIndex {
				rf.logSending = false
			}
			rf.mu.Unlock()
		}(rf.commitIndex)

		// 要发送的log entry的index
		index := len(rf.log) - 1
		copyTerm := rf.currentTerm

		DPrintf(dLog1, "S%d start replicating entry at I%d", rf.me, index)

		// 创建用于goroutine间通信的channel；创建WaitGroup，以使所有操作完成后关闭channel
		counterChan := make(chan int, len(rf.peers))
		var wg sync.WaitGroup
		wg.Add(len(rf.peers) - 1) // 添加需要等待的goroutine数（除了自己外的peers数）

		// 创建goroutine判断该entry是否复制过半，处理commit相关的操作
		go rf.checkCommit(len(rf.peers), counterChan, index)

		// 向各个节点发送日志条目
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendLogEntries(i, copyTerm, index, &wg, counterChan)
			}
		}

		// 创建收尾goroutine，等待所有日志复制尝试结束后，关闭channel
		go func() {
			wg.Wait()
			close(counterChan)
		}()
		rf.mu.Unlock()
	}
}

// 根据entries复制的结果推进commitIndex
func (rf *Raft) checkCommit(nodeNum int, c <-chan int, index int) {
	counter := 1
	// 读取counterChan中的内容直到关闭/跳出
	for i := range c {
		counter += i
		// 两整数相除，此处自动向下取整
		// 超过半数则日志可commit
		if counter > nodeNum/2 {
			rf.mu.Lock()
			// 坑：此处commit时需要检查index是否比自身的commitIndex大
			// 存在情况：并发执行多个start，后来的（commitIndex较大的）任务先完成，导致后续完成的小index覆盖原有的大index
			if index > rf.commitIndex {
				rf.commitIndex = index
			}
			DPrintf(dLog2, "S%d change commitIndex to I%d", rf.me, rf.commitIndex)
			rf.logSending = false
			rf.cond.Broadcast()
			rf.mu.Unlock()
			// 已过半，停止循环读取channel，结束该goroutine
			break
		}
	}
}

// 不断循环，判断是否有新的日志条目可以apply至状态机（发送至applyCh）
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied+1].Command,
				CommandIndex: rf.lastApplied + 1,
			}
			rf.applyCh <- msg
			rf.lastApplied++
			DPrintf(dLog2, "S%d applied (%v,I%d)", rf.me, msg.Command, msg.CommandIndex)
			// 快速重试，防止有entries等待apply
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) sendLogEntries(server int, copyTerm int, index int, wg *sync.WaitGroup, c chan<- int) {
	for i := 0; !rf.killed(); i++ {
		rf.mu.Lock()
		if rf.currentRole != leader || rf.currentTerm != copyTerm {
			rf.mu.Unlock()
			break
		}
		// 若有需要发送的日志条目
		if len(rf.log) > rf.nextIndex[server] {
			// 初始化args和reply
			// 此时的index为已经append新条目之后的index
			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = copyTerm
			args.PrevLogIndex = index - 1 - i
			args.PrevLogTerm = rf.log[index-1-i].Term
			args.LeaderCommit = rf.commitIndex
			args.Entries = rf.log[index-i:]
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			ok, success := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				break
			} else {
				if success {
					c <- 1
					rf.mu.Lock()
					rf.matchIndex[server] = index
					rf.mu.Unlock()
					break
				} else {
					continue
				}
			}
		}
		rf.mu.Unlock()
	}
	wg.Done()
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
		go rf.checkMajority(len(rf.peers), counterChan, tmpTerm)

		// 向所有其他节点发送请求投票RPC
		DPrintf(dVote, "S%d Requesting vote", rf.me)
		for i := range rf.peers {
			if i != rf.me {
				go func(copyTerm int, c chan<- int, index int) {
					rf.mu.Lock()
					// 初始化args和reply
					args := RequestVoteArgs{}
					args.CandidateId = rf.me
					args.Term = copyTerm // 不能简单地使用rf.currentTerm，因为可能已经发生改变
					args.LastLogIndex = len(rf.log) - 1
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
					reply := RequestVoteReply{}
					rf.mu.Unlock()
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

// 判断票数是否过半，并处理转换为leader的相关操作
func (rf *Raft) checkMajority(nodeNum int, c <-chan int, copyTerm int) {
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
				// 初始化nextIndex和matchIndex
				rf.nextIndex = make([]int, len(rf.peers))
				for i := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.log)
				}
				rf.matchIndex = make([]int, len(rf.peers))
				// 向所有其他节点定期发送空的AppendEntries RPC（心跳）
				// leader身份快速转变时（leader -> ? -> leader）会存在两个相同的heartbeat goroutine吗？会，且旧的goroutine
				// 错误F：leader能且只能因收到更大的term转换为follower，不可能在heartbeat timeout间完成候选与选举
				// 正确T：leader转为follower，但未给candidate投同意票，随后马上超时，成为下一任leader
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
}

// 向指定节点定期发送心跳
func (rf *Raft) heartbeatTicker(server int, copyTerm int) {
	// 坑：某个server被kill之后，ticker goroutine仍在运行，出现无限发送心跳等情况
	// 故还需检查是否已被kill
	for rf.killed() == false {
		// 若当前不再是leader，跳出循环
		// 坑：leader快速变换身份，又迅速转变回leader，导致重复的heartbeatTicker运行
		// 故还需检查任期是否为创建时的任期
		rf.mu.Lock()
		if rf.currentRole != leader || rf.currentTerm != copyTerm {
			rf.mu.Unlock()
			break
		}

		// 若当前没有日志在发送（防止冗余的一致性同步）
		if !rf.logSending {
			// 发送心跳，并完成一致性检查
			go func() {
				for i := 0; !rf.killed(); i++ {
					rf.mu.Lock()
					if rf.currentRole != leader {
						rf.mu.Unlock()
						return
					}
					index := len(rf.log) - 1 // 最后一个entry的index
					// 初始化args和reply
					args := AppendEntriesArgs{}
					args.LeaderId = rf.me
					args.Term = copyTerm // 不能简单地使用rf.currentTerm，因为可能已经发生改变
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = index - i
					args.PrevLogTerm = rf.log[index-i].Term
					args.Entries = rf.log[index-i+1:] // 最后i个entries，i为0的时候为空切片（心跳）
					reply := AppendEntriesReply{}
					rf.mu.Unlock()
					ok, success := rf.sendAppendEntries(server, &args, &reply)
					if !ok {
						break
					} else {
						if success {
							break
						}
					}
				}
			}()
		}

		rf.mu.Unlock()
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

	// 2B
	// 对log先填充一个占位元素，这样其初始长度为1，index从1开始
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	// 坑：注意初始化applyCh
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)

	// 启动log复制goroutine
	go rf.logSender()

	go rf.applier()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
