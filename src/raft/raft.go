package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
const (
	FOLLOWER           = "Follower"
	CANDIDATE          = "Candidate"
	LEADER             = "Leader"
	HEART_BEAT_TIMEOUT = 100
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// define a structure of LogEntries for further usage
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 2A
	CandidateId  int // 2A
	LastLogIndex int // 2A
	LastLogTerm  int // 2A
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 2A
	VoteGranted bool // 2A
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	heartbeatTimer *time.Timer
	electionTimer  *time.Timer
	state          string

	applyCh   chan ApplyMsg
	voteCount int

	currentTerm int
	voteFor     int
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// false if Term < currentTerm
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteCount != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// if Term > currentTerm, make current rf to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
	}

	lastLogIndex := len(rf.logs) - 1
	if args.LastLogTerm < rf.logs[lastLogIndex].Term ||
		(args.LastLogTerm == rf.logs[lastLogIndex].Term &&
			args.LastLogIndex < lastLogIndex) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.voteFor = args.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.electionTimer.Reset(randTimeDuration())
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
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
	defer rf.mu.Unlock()

	reply.Success = true
	// args.Term is out of date
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return

	}
	// rf.Term is out of date, change to Follower immediately
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
	}

	rf.electionTimer.Reset(randTimeDuration())

	lastLogIndex := len(rf.logs) - 1
	if lastLogIndex < args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	unmatchIdx := -1
	for idx := range args.Entries {
		if len(rf.logs) < (args.PrevLogIndex+2+idx) ||
			rf.logs[args.PrevLogIndex+1+idx].Term != args.Entries[idx].Term {
			unmatchIdx = idx
			break
		}
	}

	if unmatchIdx != -1 {
		rf.logs = rf.logs[:(args.PrevLogIndex + 1 + unmatchIdx)]
		rf.logs = append(rf.logs, args.Entries[unmatchIdx:]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commit(min(args.LeaderCommit, len(rf.logs)-1))
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	if isLeader {
		rf.logs = append(rf.logs, LogEntry{Command: command, Term: term})
		index = len(rf.logs) - 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.voteFor = -1
	rf.heartbeatTimer = time.NewTimer(HEART_BEAT_TIMEOUT * time.Millisecond)
	rf.electionTimer = time.NewTimer(randTimeDuration())

	rf.applyCh = applyCh
	rf.logs = make([]LogEntry, 1)

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				switch rf.state {
				case FOLLOWER:
					rf.changeState(CANDIDATE)
				case CANDIDATE:
					rf.startElection()
				}
				rf.mu.Unlock()
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == LEADER {
					rf.heartbeats()
					rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
				}
				rf.mu.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) changeState(state string) {
	if rf.state == state {
		return
	}

	rf.state = state
	switch state {
	case FOLLOWER:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration())
		rf.voteFor = -1
	case CANDIDATE:
		rf.startElection()
	case LEADER:
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.logs)
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		rf.electionTimer.Stop()
		rf.heartbeats()
		rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
	}
}

func (rf *Raft) heartbeats() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.heartbeat(i)
		}
	}
}

func (rf *Raft) heartbeat(server int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1

	entires := make([]LogEntry, len(rf.logs[prevLogIndex+1:]))
	copy(entires, rf.logs[prevLogIndex+1:])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex].Term,
		Entries:      entires,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(entires)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
				cnt := 0
				for _, matchIdx := range rf.matchIndex {
					if matchIdx >= N {
						cnt += 1
					}
				}
				if cnt > len(rf.peers)/2 {
					rf.commit(N);
					break
				}
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.changeState(FOLLOWER)
			} else {
				rf.nextIndex[server] = args.PrevLogIndex - 1
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.voteCount = 1
	rf.electionTimer.Reset(randTimeDuration())

	for i := range rf.peers {
		if i != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				lastLogIndex := len(rf.logs) - 1
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  rf.logs[lastLogIndex].Term,
				}
				rf.mu.Unlock()
				var reply RequestVoteReply
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.changeState(FOLLOWER)
					}
					if reply.VoteGranted && rf.state == CANDIDATE {
						rf.voteCount += 1
						if rf.voteCount > len(rf.peers)/2 {
							rf.changeState(LEADER)
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) commit(commitIdx int) {
	rf.commitIndex = commitIdx
	if rf.commitIndex > rf.lastApplied {
		entriesToApply := append([]LogEntry{}, rf.logs[(rf.lastApplied+1):(rf.commitIndex+1)]...)
		go func(startIdx int, entires []LogEntry) {
			for idx, entry := range entires {
				var msg ApplyMsg
				msg.Command = entry.Command
				msg.CommandIndex = startIdx + idx
				msg.CommandValid = true
				rf.applyCh <- msg
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, entriesToApply)
	}

}
func randTimeDuration() time.Duration {
	return time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
}

func min(x, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}
