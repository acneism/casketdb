package resp

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

type Writer struct {
	Proto int
	bw    *bufio.Writer
	num   []byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{Proto: 2, bw: bufio.NewWriterSize(w, 16<<10)}
}

func (w *Writer) Simple(s string) {
	w.line('+', s)
}

func (w *Writer) Error(s string) {
	w.line('-', s)
}

func (w *Writer) line(p byte, s string) {
	w.bw.WriteByte(p)
	w.bw.WriteString(oneLine(s))
	w.bw.WriteString("\r\n")
}

func (w *Writer) Integer(n int64) {
	w.prefixed(':', n)
}

func (w *Writer) Bulk(b []byte) {
	w.prefixed('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

func (w *Writer) BulkString(s string) {
	w.prefixed('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

func (w *Writer) Null() {
	if w.Proto == 3 {
		w.bw.WriteString("_\r\n")
		return
	}
	w.bw.WriteString("$-1\r\n")
}

func (w *Writer) Array(n int) {
	w.prefixed('*', int64(n))
}

func (w *Writer) NullArray() {
	if w.Proto == 3 {
		w.bw.WriteString("_\r\n")
		return
	}
	w.bw.WriteString("*-1\r\n")
}

func (w *Writer) Map(n int) {
	if w.Proto == 3 {
		w.prefixed('%', int64(n))
		return
	}
	w.prefixed('*', 2*int64(n))
}

func (w *Writer) Set(n int) {
	w.aggregate('~', n)
}

func (w *Writer) Push(n int) {
	w.aggregate('>', n)
}

func (w *Writer) aggregate(p byte, n int) {
	if w.Proto == 3 {
		w.prefixed(p, int64(n))
		return
	}
	w.prefixed('*', int64(n))
}

func (w *Writer) Double(s string) {
	if w.Proto == 3 {
		w.line(',', s)
		return
	}
	w.BulkString(s)
}

func (w *Writer) Verbatim(s string) {
	if w.Proto == 3 {
		w.prefixed('=', int64(len(s)+4))
		w.bw.WriteString("txt:")
		w.bw.WriteString(s)
		w.bw.WriteString("\r\n")
		return
	}
	w.BulkString(s)
}

func (w *Writer) Flush() error {
	return w.bw.Flush()
}

func (w *Writer) prefixed(p byte, n int64) {
	w.num = append(w.num[:0], p)
	w.num = strconv.AppendInt(w.num, n, 10)
	w.num = append(w.num, '\r', '\n')
	w.bw.Write(w.num)
}

func oneLine(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}
