package raft

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type logTopic string

const (
	dLeader    logTopic = "LEAD"
	dLog1      logTopic = "LOG1"
	dTerm      logTopic = "TERM"
	dTimer     logTopic = "TIMR"
	dVote      logTopic = "VOTE"
	dLog2      logTopic = "LOG2"
	dPersist   logTopic = "PERS"
	dSnapshot  logTopic = "SNAP"
	dBootstrap logTopic = "BOOT"
	dTest      logTopic = "TEST"
	dError     logTopic = "ERRO"
)

const (
	colorRed   = "\033[31m"
	colorReset = "\033[0m"
)

const Debug = true

var debugStart time.Time
var dMu sync.Mutex

func DebugInit() {
	log.SetFlags(0)
	dMu.Lock()
	debugStart = time.Now()
	dMu.Unlock()
}

// 记录/打印日志输出
// 格式："time(ms) type who what"
func DPrintf(topic logTopic, format string, a ...interface{}) {
	if Debug {
		dMu.Lock()
		timestamp := time.Since(debugStart).Milliseconds()
		dMu.Unlock()
		prefix := fmt.Sprintf("%08d %v ", timestamp, topic)
		format = prefix + format
		if topic == dError {
			log.Printf(colorRed+format+colorReset, a...)
		} else {
			log.Printf(format, a...)
		}
	}
}

// 记录/打印日志输出（详细信息）
// 使用时需保证所有输出语句均加锁
// 格式："time(ms) type who what {content of rf}"
func (rf *Raft) DPrintf(topic logTopic, format string, a ...interface{}) {
	if Debug {
		dMu.Lock()
		timestamp := time.Since(debugStart).Milliseconds()
		dMu.Unlock()
		prefix := fmt.Sprintf("%08d %v ", timestamp, topic)
		format = prefix + format
		format += fmt.Sprintf("%+v", rf)
		if topic == dError {
			log.Printf(colorRed+format+colorReset, a...)
		} else {
			log.Printf(format, a...)
		}
	}
}
