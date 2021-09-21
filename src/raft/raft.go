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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	n 		  int				// number of raft servers

	// Non-volatile fields on all servers
	//
	// Lastest term server has seen
	// (Initialized to 0 on first boot, increases monotonically)
	currentTerm	int
	// CandidateId that received vote in current term (or null/-1 if none)
	votedFor	int
	// LogEntry entries; each entry contains command for state machine, and term when
	// entry was received by leader (first index is 1)
	logs		[]LogEntry

	// Volatile fields on all servers
	//
	// Index of the highest log entry known to be committed
	// (Initialized to 0, increases monotonically)
	commitIndex	int
	// Index of the highest log entry applied to state machine
	// (Initialized to 0, increases monotonically)
	lastApplied int


	// Refer to who is leader
	currentLeaderId int
	// Current raft node state
	currentState	State
	//	Reset election timeout ticker when received heartbeat from server
	resetElectionChan chan int
	//	Number of granted votes after received RequestVoteRPC reply
	numberOfGrantedVotes int32

	// Volatile fields on leader, reinitialized after election
	//
	// For each server, index of the highest log entry to send to that server
	// (Initialized to leader last log index + 1)
	nextIndex	[]int
	// For each server, index of the highest log entry known to be
	// replicated on server
	// (Initialized to 0, increases monotonically)
	matchIndex	[]int

	// Handle AppendEntriesRPC reply int this channel
	appendEntriesReplyHandler chan AppendEntriesReply
	// Use this handler to stop leader logic
	stopLeaderLogicHandler 	chan int

	// Handle RequestVoteRPC reply in this channel
	requestVoteReplyHandler chan RequestVoteReply
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == StateLeader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int 	// candidate's term
	CandidateId		int		// candidate requesting vote
	LastLogIndex 	int		// index of candidate's last log entry
	LastLogTerm 	int		// term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	PeerId      int  // indicates who's vote
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("Raft node (%d) handles with RequestVote, candidateId: %v\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.PeerId = rf.me

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("Raft node (%v) denied vote, votedFor: %v, candidateId: %v.\n", rf.me,
			rf.votedFor, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	lastLogIndex := len(rf.logs) - 1
	lastLogEntry := rf.logs[lastLogIndex]
	if lastLogEntry.Term > args.LastLogTerm || lastLogIndex > args.LastLogIndex {
		// If this node is more up-to-date than candidate, then reject vote
		//DPrintf("Raft node (%v) LastLogIndex: %v, LastLogTerm: %v, args (%v, %v)\n", rf.me,
		//	lastLogIndex, lastLogEntry.Term, args.LastLogIndex, args.LastLogTerm)
		reply.VoteGranted = false
		return
	}

	rf.tryEnterFollowState(args.Term)

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
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
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.PeerId = rf.me

	if rf.currentTerm > args.Term {
		// 1. Reply false if term < currentTerm
		return
	}

	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	if prevLogIndex >= len(rf.logs) {
		return
	}
	if rf.logs[prevLogIndex].Term != prevLogTerm {
		// 2. Reply false if log dosen't contain an entry at prevLogIndex
		// 	  whose term matches prevLogTerm
		return
	}

	var i, j = 0, prevLogIndex + 1
	for ; i < len(args.Entries) && j < len(rf.logs); i, j = i + 1, j + 1 {
		if rf.logs[j].Term != args.Entries[i].Term {
			// 3. If an existing entry conflicts with a new one (same index but different terms),
			// 	  delete the existing entry and all that follow it.
			rf.logs = rf.logs[:j]
			break
		}
	}

	// 4. Append any new entries not already in the log
	for ; i < len(args.Entries); i++ {
		rf.logs = append(rf.logs, args.Entries[i])
	}

	if args.LeaderCommit > rf.commitIndex {
		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = Min(args.LeaderCommit, len(rf.logs) - 1)
	}

	rf.tryEnterFollowState(args.Term)

	rf.votedFor = -1
	rf.currentLeaderId = args.LeaderId
	// Reset election timer after received AppendEntries
	rf.resetElectionChan <- 1
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index = rf.commitIndex + 1
	term = rf.currentTerm
	isLeader = rf.currentState == StateLeader
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	z := atomic.LoadInt32((*int32)(&rf.currentState))
	return State(z) == StateLeader
}

func (rf *Raft) isCandidate() bool {
	z := atomic.LoadInt32((*int32)(&rf.currentState))
	return State(z) == StateCandidate
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <- rf.resetElectionChan:
			DPrintf("Raft node (%v) reseted election timer, currentLeaderId: %v, currentTerm: %v, currentState: %v, votedFor: %v.\n",
				rf.me, rf.currentLeaderId, rf.currentTerm, rf.currentState, rf.votedFor)
			// Reset election timeout
		case <- time.After(time.Millisecond * time.Duration(RandInt(MinElectionTimeout, MaxElectionTimeout))):
			if !rf.isLeader() {
				rf.enterCandidateState()
			} else {
				// If a leader didn't receive any AppendEntries reply from peers, it reverts to follower state.
				rf.mu.Lock()
				rf.tryEnterFollowState(rf.currentTerm)
				rf.mu.Unlock()
				rf.stopLeaderLogicHandler <- 1
			}
		}
	}
}

// ---------------------------------------------------------------------------------------------------
// Follow logic

// Revert Raft node into follow state,
// after received RPC from other nodes and find its own term is out of date
// **must be called in lock**
func (rf *Raft) tryEnterFollowState(term int) {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower.
	if term >= rf.currentTerm && rf.currentState != StateFollower {
		DPrintf("Raft node (%v) reverted into follower, term: %v.\n", rf.me, term)
		rf.votedFor = -1
		rf.currentTerm = term
		rf.currentState = StateFollower
	}
}

// ---------------------------------------------------------------------------------------------------
// Candidate logic

// Revert Raft node into candidate state
func (rf *Raft) enterCandidateState()  {
	rf.mu.Lock()
	rf.currentState = StateCandidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.numberOfGrantedVotes = int32(1)
	rf.mu.Unlock()
	DPrintf("Raft node (%v) reverted into candidate, currentTerm: %v.\n", rf.me, rf.currentTerm)

	// First, start a goroutine to handle RequestVote reply
	go rf.handleRequestVoteReply()

	for ii :=range rf.peers {
		if ii == rf.me {
			continue
		}

		go func(i int) {
			// Initialize RequestVoteArgs
			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.logs) - 1
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term

			reply := RequestVoteReply{}

			rf.sendRequestVote(i, &args, &reply)
			// Handle RequestVoteRPC reply
			rf.requestVoteReplyHandler <- reply
		}(ii)
	}
}

func (rf *Raft) handleRequestVoteReply() {
	for rf.isCandidate() {
		select {
		case reply := <-rf.requestVoteReplyHandler:
			if !rf.isCandidate() {
				return
			}
			DPrintf("Raft node (%v) received RequestVote reply, PeerId: %v, VoteGranted: %v, Term: %v\n",
				rf.me, reply.PeerId, reply.VoteGranted, reply.Term)
			if reply.VoteGranted == true {
				nOfGrantedVotes := atomic.AddInt32(&rf.numberOfGrantedVotes, int32(1))
				if int(nOfGrantedVotes) >= rf.n>>1 + 1 {
					// Hold a majority of raft servers's votes
					// enter into state of Leader
					rf.enterLeaderState()
					return
				}
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.tryEnterFollowState(reply.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}
	}
}

// ---------------------------------------------------------------------------------------------------
// Leader logic
func (rf *Raft) enterLeaderState() {
	DPrintf("Raft node (%v) reverted into leader.\n", rf.me)
	rf.mu.Lock()
	rf.currentState = StateLeader
	rf.currentLeaderId = rf.me
	// When a leader first comes to power, it initializes all nextIndex values
	// to the index just after the last one in its log.
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
	}
	rf.mu.Unlock()

	go rf.handleAppendEntriesReply()
	// Send heartbeat to followers
	go rf.pulse()
}

// Send heartbeat to followers, only executed by leader
func (rf *Raft) pulse() {
	for rf.isLeader() {
		// Start send AppendEntries RPC to the rest of cluster
		for ii := range rf.peers {
			if ii == rf.me {
				continue
			}
			go func(i int) {
				args := rf.makeAppendEntriesArgs(i)
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, &args, &reply)
				rf.appendEntriesReplyHandler <- reply
			}(ii)
		}

		time.Sleep(time.Duration(HeartBeatsInterval) * time.Millisecond)
	}
}

func (rf *Raft) handleAppendEntriesReply() {
	for rf.isLeader() {
		select {
		case <- rf.stopLeaderLogicHandler:
			// Break out this loop
		case reply := <- rf.appendEntriesReplyHandler:
			if !rf.isLeader() {
				return
			}

			DPrintf("Raft node (%v) starts to handle AppendEntries reply.\n", rf.me)
			rf.mu.Lock()

			peerId := reply.PeerId
			if reply.Success {
				// Reset election timer to prevent reverting leader to follower
				rf.resetElectionChan <- 1

				// TODO update	nextIndex[], matchIndex[]
				rf.nextIndex[peerId] = len(rf.logs)
				rf.matchIndex[peerId] = rf.commitIndex
			} else {
				if reply.Term > rf.currentTerm {
					rf.tryEnterFollowState(reply.Term)
					rf.mu.Unlock()
					break
				}

				// TODO Sync logs
				//rf.nextIndex[peerId] -= 1
				//go func(pId int) {
				//	args := rf.makeAppendEntriesArgs(pId)
				//	rep := AppendEntriesReply{}
				//	rf.sendAppendEntries(peerId, &args, &rep)
				//	rf.appendEntriesReplyHandler <- rep
				//}(peerId)
			}

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) makeAppendEntriesArgs(peerId int) AppendEntriesArgs {
	// Initialize AppendEntriesArgs
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[peerId] - 1
	if args.PrevLogIndex > 0 {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	}
	args.Entries = []LogEntry{}
	args.LeaderCommit = rf.commitIndex
	return args
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
	rf.n = len(peers)
	rf.currentState = StateFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.logs[0] = LogEntry{
		Term: 0,
		Type: NoOp,
		Command: nil,
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.resetElectionChan = make(chan int)
	rf.numberOfGrantedVotes = 0
	rf.nextIndex = make([]int, rf.n)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, rf.n)

	rf.appendEntriesReplyHandler = make(chan AppendEntriesReply)
	rf.stopLeaderLogicHandler = make(chan int)
	rf.requestVoteReplyHandler = make(chan RequestVoteReply)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	DPrintf("Raft node (%v) starts up...\n", me)
	return rf
}
