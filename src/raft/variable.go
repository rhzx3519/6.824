package raft

// Raft state
type State int32
const (
	StateFollower	State =	1 << iota
	StateCandidate
	StateLeader
	StateDead
)

//
// Raft log entry
//
type LogType int
const (
	NoOp 	LogType = 1 << iota		// for heartbeat
	Command							//
)

type LogEntry struct {
	Term 		int		// term when entry received by the leader (first index is 1)
	Type 		LogType
	Command 	interface{}
}

//
// AppendEntries RPC
//
type AppendEntriesArgs struct {
	Term 			int			// leader's term
	LeaderId 		int			// so follower can redirect clients
	PrevLogIndex	int			// index of log entry immediately preceding new ones

	PrevLogTerm		int			// term of prevLogIndex entry
	Entries			[]LogEntry	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int			// leader's commitIndex
}

type AppendEntriesReply struct {
	PeerId  int  // Raft node id
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

//
// Election timeout	in millis
//
const (
	MinElectionTimeout = 150		// minimum election timeout (ms)
	MaxElectionTimeout = 300		// maximum election timeout (ms)
)

//
// Heartbeats interval in millis
const HeartBeatsInterval = 30	// (ms)

