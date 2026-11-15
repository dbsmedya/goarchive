package control

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dbsmedya/goarchive/v3/internal/tuning"
)

// The wire protocol is a public contract (spec §4.3, D4, D19). One request per connection, read up
// to its first LF; the reply is `OK` LF and a body, or exactly one line `ERR <reason> <message>`
// LF; the server writes its reply and closes. This file is the framing and the lexical form; the
// meaning of a request is commands.go's.

// protocolVersion is the contract's version. help reports it as its first body line and
// status-json as "protocol".
const protocolVersion = 1

// maxRequest is the longest request in bytes, its LF included.
const maxRequest = 4096

// The reason words the server judges itself, before it asks the store; the store's are
// tuning.Reason's.
const (
	reasonDenied    tuning.Reason = "denied"
	reasonMalformed tuning.Reason = "malformed"
	reasonUnknown   tuning.Reason = "unknown"
)

// errNoLineFeed and errTooLong are the two ways a stream is not a request: it ended before an
// LF, or it went on for maxRequest bytes without one. Both are `malformed`. Any other error of a
// read (a reset, a passed deadline) is the transport's, and says nothing about the request.
var (
	errNoLineFeed = errors.New("the request ended before a line feed")
	errTooLong    = fmt.Errorf("the request is longer than %d bytes", maxRequest)
)

// readRequest reads the first line of r: up to its first LF, at most maxRequest bytes with it. It
// returns the line without the LF. What follows the first LF is not a request: it is never read
// further, never parsed and never refused, so a tail cannot undo or invalidate the first line's
// change, and the outcome does not depend on packet timing.
func readRequest(r io.Reader) (string, error) {
	buf := make([]byte, maxRequest)
	n := 0
	for {
		m, err := r.Read(buf[n:])
		if i := bytes.IndexByte(buf[n:n+m], '\n'); i >= 0 {
			return string(buf[:n+i]), nil
		}
		n += m
		if n >= maxRequest {
			return "", errTooLong
		}
		switch {
		case errors.Is(err, io.EOF):
			return "", errNoLineFeed
		case err != nil:
			return "", err
		}
	}
}

// kind is what a request asks for.
type kind int

const (
	// kindAction is a bare token: help, status, pause and the rest.
	kindAction kind = iota
	// kindSet is token=number.
	kindSet
	// kindGet is token=?.
	kindGet
)

// request is a line that met the lexical form. Whether its name exists is a second step.
type request struct {
	name   string
	kind   kind
	number string
}

// malformedError is a line that does not have the lexical form.
type malformedError struct{ line string }

func (e *malformedError) Error() string {
	return "the request is not of the form token, token=number or token=?: " + quoted(e.line)
}

// parseRequest judges the lexical form of one line (spec §4.3):
//
//	line   = token | token "=" number | token "=?"
//	token  = lower *( lower | digit | "_" | "-" )
//	number = 1*digit [ "." 1*digit ]
//
// It is defined without reference to which names exist. Nothing is trimmed into validity: a space,
// a tab, a CR, an uppercase letter, a sign, an exponent or an empty line breaks the form.
func parseRequest(line string) (request, error) {
	name, value, hasEquals := strings.Cut(line, "=")
	if !isToken(name) {
		return request{}, &malformedError{line}
	}
	switch {
	case !hasEquals:
		return request{name: name, kind: kindAction}, nil
	case value == "?":
		return request{name: name, kind: kindGet}, nil
	case isNumber(value):
		return request{name: name, kind: kindSet, number: value}, nil
	}
	return request{}, &malformedError{line}
}

func isToken(s string) bool {
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		return false
	}
	for i := 1; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func isNumber(s string) bool {
	whole, fraction, hasPoint := strings.Cut(s, ".")
	return isDigits(whole) && (!hasPoint || isDigits(fraction))
}

// quoted shows a candidate on one line, cut to 64 bytes (spec §4.10).
func quoted(s string) string {
	if len(s) > 64 {
		s = s[:64] + "..."
	}
	return strconv.Quote(s)
}

// reply is the server's answer to one request.
type reply struct {
	ok      bool
	body    []string
	reason  tuning.Reason
	message string
}

func okReply(body ...string) reply { return reply{ok: true, body: body} }

func errReply(reason tuning.Reason, message string) reply {
	return reply{reason: reason, message: message}
}

var oneLine = strings.NewReplacer("\n", " ", "\r", " ")

// wire is the reply as it goes on the connection: every line ends with LF, and an ERR is exactly
// one line, so a message's line breaks become spaces.
func (r reply) wire() []byte {
	if !r.ok {
		return []byte("ERR " + string(r.reason) + " " + oneLine.Replace(r.message) + "\n")
	}
	var b bytes.Buffer
	b.WriteString("OK\n")
	for _, line := range r.body {
		b.WriteString(oneLine.Replace(line))
		b.WriteByte('\n')
	}
	return b.Bytes()
}
