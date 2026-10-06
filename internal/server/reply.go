package server

import (
	"errors"

	"github.com/acneism/casketdb/internal/replica"
	"github.com/acneism/casketdb/internal/resp"
)

type reply interface {
	writeTo(w *resp.Writer)
}

type statusReply string

type errorReply string

type intReply int64

type bulkReply []byte

type nullReply struct{}

type nullArrayReply struct{}

type arrayReply []reply

type stringsReply []string

var (
	okReply  = statusReply("OK")
	nilReply = nullReply{}
)

func (r statusReply) writeTo(w *resp.Writer) { w.Simple(string(r)) }

func (r errorReply) writeTo(w *resp.Writer) { w.Error(string(r)) }

func (r intReply) writeTo(w *resp.Writer) { w.Integer(int64(r)) }

func (r bulkReply) writeTo(w *resp.Writer) { w.Bulk(r) }

func (nullReply) writeTo(w *resp.Writer) { w.Null() }

func (nullArrayReply) writeTo(w *resp.Writer) { w.NullArray() }

func (r arrayReply) writeTo(w *resp.Writer) {
	w.Array(len(r))
	for _, e := range r {
		e.writeTo(w)
	}
}

func (r stringsReply) writeTo(w *resp.Writer) {
	w.Array(len(r))
	for _, s := range r {
		w.BulkString(s)
	}
}

type mapReply []reply

type setReply []reply

type pushReply []reply

type pairsReply []reply

type keyedReply []reply

type doubleReply string

type verbatimReply string

func (r mapReply) writeTo(w *resp.Writer) {
	w.Map(len(r) / 2)
	for _, e := range r {
		e.writeTo(w)
	}
}

func (r setReply) writeTo(w *resp.Writer) {
	w.Set(len(r))
	for _, e := range r {
		e.writeTo(w)
	}
}

func (r pushReply) writeTo(w *resp.Writer) {
	w.Push(len(r))
	for _, e := range r {
		e.writeTo(w)
	}
}

func (r pairsReply) writeTo(w *resp.Writer) {
	if w.Proto != 3 {
		arrayReply(r).writeTo(w)
		return
	}
	writePairs(w, r)
}

func (r keyedReply) writeTo(w *resp.Writer) {
	if w.Proto == 3 {
		mapReply(r).writeTo(w)
		return
	}
	writePairs(w, r)
}

func writePairs(w *resp.Writer, r []reply) {
	w.Array(len(r) / 2)
	for i := 0; i+1 < len(r); i += 2 {
		w.Array(2)
		r[i].writeTo(w)
		r[i+1].writeTo(w)
	}
}

func (r doubleReply) writeTo(w *resp.Writer) { w.Double(string(r)) }

func (r verbatimReply) writeTo(w *resp.Writer) { w.Verbatim(string(r)) }

func storageError(err error) errorReply {
	switch {
	case errors.Is(err, replica.ErrNotLeader):
		return errorReply("READONLY You can't write against a read only replica.")
	case errors.Is(err, replica.ErrUnconfirmed):
		return errorReply("TRYAGAIN No leader confirmed the read, retry.")
	case errors.Is(err, replica.ErrRemoved):
		return errorReply("ERR this node was removed from the cluster")
	}
	return errorReply("ERR " + err.Error())
}
