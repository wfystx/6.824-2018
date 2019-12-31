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
	Term         int	// 2A
	CandidateId  int	// 2A
	LastLogIndex int	// 2A
	LastLogTerm  int	// 2A
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int		// 2A
	VoteGranted bool	// 2A
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

	timer   *time.Timer
	timeout time.Duration
	state   string

	appendCh  chan bool
	voteCh    chan bool
	voteCount int

	currentTerm int
	voteFor     int
	logs		[]LogEntry

	commitIndex	int
	lastApplied	int

	nextIndex	[]int
	matchIndex	[]int
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

	reply.VoteGranted = false

	// false if Term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// if Term > currentTerm, make current rf to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
	}

	// Otherwise, if rf didn't give others vote
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
	}

	// match term to reply
	reply.Term = rf.currentTerm
	go func() {
		rf.voteCh <- true
	}()
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
		// rf.Term is out of date, change to Follower immediately
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
	}

	go func() {
		rf.appendCh <- true
	}()
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
	DPrintf("Making raft%v...\n", rf.me)
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.voteCh = make(chan bool)
	rf.appendCh = make(chan bool)

	electionTimeout := HEART_BEAT_TIMEOUT * 3 + rand.Intn(HEART_BEAT_TIMEOUT)
	rf.timeout = time.Duration(electionTimeout) * time.Millisecond
	rf.timer = time.NewTimer(rf.timeout)

	go func() {
		for {
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			electionTimeout := HEART_BEAT_TIMEOUT * 3 + rand.Intn(HEART_BEAT_TIMEOUT)
			rf.timeout = time.Duration(electionTimeout) * time.Millisecond

			switch state{
			case FOLLOWER:
				select {
				case <- rf.appendCh:
					rf.timer.Reset(rf.timeout)
				case <- rf.voteCh:
					rf.timer.Reset(rf.timeout)
				case <- rf.timer.C:
					rf.mu.Lock()
					rf.changeState(CANDIDATE)
					rf.mu.Unlock()
					rf.startElection()
				}
			case CANDIDATE:
				select {
				case <- rf.appendCh:
					rf.timer.Reset(rf.timeout)
					rf.mu.Lock()
					rf.changeState(FOLLOWER)
					rf.mu.Unlock()
				case <- rf.timer.C:
					rf.startElection()
				default:
					rf.mu.Lock()
					//fmt.Printf("%v\n", rf.voteCount)
					if rf.voteCount > len(rf.peers) / 2 {
						rf.changeState(LEADER)
					}
					rf.mu.Unlock()
				}
			case LEADER:
				rf.heartbeats()
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

	if state == FOLLOWER {
		rf.state = FOLLOWER
		DPrintf("Raft%v change to Follower in Term %v\n", rf.me, rf.currentTerm)
		rf.voteFor = -1
	}
	if state == CANDIDATE {
		rf.state = CANDIDATE
		DPrintf("Raft%v change to Candidate in Term %v\n", rf.me, rf.currentTerm)
	}
	if state == LEADER {
		rf.state = LEADER
		DPrintf("Raft%v change to Leader in Term %v\n", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) heartbeats() {
	for peer, _ := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				rf.mu.Unlock()
				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(peer, &args, &reply) {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.changeState(FOLLOWER)
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
	time.Sleep(HEART_BEAT_TIMEOUT * time.Millisecond)
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.voteFor = rf.me
	rf.timer.Reset(rf.timeout)
	rf.voteCount = 1
	rf.mu.Unlock()

	for peer, _ := range rf.peers {
		if peer != rf.me {
			go func(peer int) {
				rf.mu.Lock()
				args := RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				rf.mu.Unlock()

				reply := RequestVoteReply{}
				if rf.sendRequestVote(peer, &args, &reply) {
					rf.mu.Lock()
					if reply.VoteGranted {
						rf.voteCount += 1
					} else if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.changeState(FOLLOWER)
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
	}
}
