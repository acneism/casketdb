package server

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"

	"github.com/acneism/casketdb/internal/replica"
)

var raftCommands = map[string]command{
	"raft": {arity: -2, kind: kindConn, acl: catAdmin | catDangerous, conn: cmdRaft},
}

func cmdRaft(s *Server, c *client, args [][]byte) reply {
	rep := s.cfg.Replica
	if rep == nil {
		return errorReply("ERR RAFT needs a cluster, start the node with -raft-id")
	}
	var err error
	var out reply = okReply
	switch sub := upper(args[1]); {
	case sub == "MEMBERS" && len(args) == 2:
		var out arrayReply
		for _, m := range rep.Members() {
			role := "learner"
			if m.Voter {
				role = "voter"
			}
			out = append(out, stringsReply{m.ID, m.Addr, role})
		}
		return out
	case sub == "TRANSFER" && len(args) <= 3:
		to := ""
		if len(args) == 3 {
			to = string(args[2])
		}
		err = rep.TransferLeadership(to)
	case sub == "ADDLEARNER" && len(args) == 4:
		id, addr := string(args[2]), string(args[3])
		if err = rep.AddLearner(id, addr); err == nil {
			peers := id + "=" + addr
			for _, m := range rep.Members() {
				if m.ID != id {
					peers += "," + m.ID + "=" + m.Addr
				}
			}
			out = bulkReply(peers)
		}
	case sub == "PROMOTE" && len(args) == 3:
		err = rep.Promote(string(args[2]))
	case sub == "REMOVE" && len(args) == 3:
		err = rep.Remove(string(args[2]))
	default:
		return unknownSubcommand(args)
	}
	if err != nil {
		return raftError(rep, err)
	}
	s.audit(c, slog.LevelInfo, "RAFT command", c.user.name, "command", string(bytes.Join(args[1:], []byte(" "))))
	return out
}

func raftError(rep *replica.Node, err error) errorReply {
	if !errors.Is(err, replica.ErrNotLeader) {
		return errorReply("ERR " + err.Error())
	}
	st := rep.Status()
	if st.LeaderID == "" {
		return errorReply("ERR this node is not the leader and no leader is known, retry")
	}
	return errorReply(fmt.Sprintf("ERR this node is not the leader, run it on %s (raft address %s)", st.LeaderID, st.LeaderAddr))
}
