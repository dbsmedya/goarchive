package types

import (
	"fmt"
	"github.com/dbsmedya/dbsgomysql/pkg/sqlutil"
	"strconv"
	"strings"
)

// TemporalKind identifies application columns which must be transported as SQL text.
type TemporalKind uint8

const (
	TemporalNone TemporalKind = iota
	TemporalDate
	TemporalDateTime
	TemporalTimestamp
)

// ColumnMetadata retains the inspector's exact ordered column names and known temporal facts.
type ColumnMetadata struct {
	Names    []string
	Temporal map[string]TemporalKind
}

func (m ColumnMetadata) Kind(column string) TemporalKind {
	for name, kind := range m.Temporal {
		if asciiEqual(name, column) {
			return kind
		}
	}
	return TemporalNone
}
func asciiEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x >= 'A' && x <= 'Z' {
			x += 'a' - 'A'
		}
		if y >= 'A' && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
func (m ColumnMetadata) Projection() (string, error) {
	if len(m.Names) == 0 || m.Temporal == nil {
		return "", &TemporalReadContractError{Operation: "metadata", Detail: "missing column metadata"}
	}
	parts := make([]string, len(m.Names))
	for i, name := range m.Names {
		p, err := m.ProjectColumn(name)
		if err != nil {
			return "", err
		}
		parts[i] = p
	}
	return strings.Join(parts, ", "), nil
}
func (m ColumnMetadata) ProjectColumn(column string) (string, error) {
	fail := func(detail string) (string, error) {
		return "", &TemporalReadContractError{Operation: "metadata", Detail: detail}
	}
	if len(m.Names) == 0 || m.Temporal == nil {
		return fail("missing column metadata")
	}
	for _, name := range m.Names {
		if name != "" && asciiEqual(name, column) {
			q := sqlutil.QuoteIdentifier(name)
			switch m.Kind(name) {
			case TemporalNone:
				return q, nil
			case TemporalDate, TemporalDateTime, TemporalTimestamp:
				return "CAST(" + q + " AS CHAR) AS " + q, nil
			default:
				return fail("invalid temporal classification")
			}
		}
	}
	return fail("unknown column " + column)
}

// TemporalText copies SQL text out of either supported driver container.
func TemporalText(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	default:
		return "", &TemporalReadContractError{Operation: "raw-read", Detail: fmt.Sprintf("expected temporal SQL text, received %T", value)}
	}
}

// ValidateTemporalKey checks a key without normalizing or changing its spelling.
func ValidateTemporalKey(kind TemporalKind, text string) error {
	fail := func() error {
		return &TemporalReadContractError{Operation: "key", Detail: "unsupported temporal identity: require a valid nonzero Gregorian date and valid clock"}
	}
	if kind != TemporalDate && kind != TemporalDateTime && kind != TemporalTimestamp {
		return fail()
	}
	if len(text) < 10 || text[4] != '-' || text[7] != '-' {
		return fail()
	}
	number := func(start, end int) (int, bool) {
		for _, c := range []byte(text[start:end]) {
			if c < '0' || c > '9' {
				return 0, false
			}
		}
		n, e := strconv.Atoi(text[start:end])
		return n, e == nil
	}
	y, okY := number(0, 4)
	m, okM := number(5, 7)
	d, okD := number(8, 10)
	if !okY || !okM || !okD || y < 1 || m < 1 || m > 12 || d < 1 {
		return fail()
	}
	days := [12]int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
		days[1] = 29
	}
	if d > days[m-1] {
		return fail()
	}
	if kind == TemporalDate {
		if len(text) != 10 {
			return fail()
		}
		return nil
	}
	if len(text) < 19 || text[10] != ' ' || text[13] != ':' || text[16] != ':' {
		return fail()
	}
	h, okH := number(11, 13)
	mi, okMi := number(14, 16)
	s, okS := number(17, 19)
	if !okH || !okMi || !okS || h > 23 || mi > 59 || s > 59 {
		return fail()
	}
	if len(text) > 19 {
		if len(text) < 21 || len(text) > 26 || text[19] != '.' {
			return fail()
		}
		if _, ok := number(20, len(text)); !ok {
			return fail()
		}
	}
	return nil
}

// TemporalReadContractError refuses an unproved application-data read identity.
type TemporalReadContractError struct {
	Table, Operation, Detail string
	Cause                    error
}

func (e *TemporalReadContractError) Error() string {
	message := fmt.Sprintf("TEMPORAL_READ_CONTRACT: table=%s operation=%s: %s", e.Table, e.Operation, strings.NewReplacer("\n", " ", "\r", " ").Replace(e.Detail))
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}
func (e *TemporalReadContractError) Unwrap() error { return e.Cause }

// TemporalIdentitySet requires a fetched raw identity set to equal the requested set.
type TemporalIdentitySet struct {
	kind             TemporalKind
	table, operation string
	requested, seen  map[string]struct{}
	keys             []interface{}
}

func NewTemporalIdentitySet(kind TemporalKind, table, operation string, requested []interface{}) (*TemporalIdentitySet, error) {
	s := &TemporalIdentitySet{kind: kind, table: table, operation: operation, requested: map[string]struct{}{}, seen: map[string]struct{}{}}
	if kind == TemporalNone {
		return nil, s.fail("non-temporal identity kind", nil)
	}
	for _, v := range requested {
		key, ok := v.(string)
		if !ok {
			return nil, s.fail("requested temporal identity must be a string", nil)
		}
		if err := ValidateTemporalKey(kind, key); err != nil {
			return nil, s.fail("invalid requested identity", err)
		}
		if _, exists := s.requested[key]; !exists {
			s.requested[key] = struct{}{}
			s.keys = append(s.keys, key)
		}
	}
	return s, nil
}
func (s *TemporalIdentitySet) fail(detail string, cause error) error {
	return &TemporalReadContractError{Table: s.table, Operation: s.operation, Detail: detail, Cause: cause}
}
func (s *TemporalIdentitySet) Observe(value interface{}) error {
	key, err := TemporalText(value)
	if err != nil {
		return s.fail("unfaithful fetched representation", err)
	}
	if err = ValidateTemporalKey(s.kind, key); err != nil {
		return s.fail("invalid fetched identity", err)
	}
	if _, ok := s.requested[key]; !ok {
		return s.fail("unexpected fetched identity", nil)
	}
	if _, ok := s.seen[key]; ok {
		return s.fail("duplicate fetched identity", nil)
	}
	s.seen[key] = struct{}{}
	return nil
}
func (s *TemporalIdentitySet) Finish() error {
	if len(s.seen) != len(s.requested) {
		return s.fail(fmt.Sprintf("requested=%d fetched=%d", len(s.requested), len(s.seen)), nil)
	}
	return nil
}
func (s *TemporalIdentitySet) Keys() []interface{} { return append([]interface{}(nil), s.keys...) }
