package key

import (
	"database/sql"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/dbsmedya/goarchive/v3/internal/types"
)

// Key is one row's primary-key value: one element per key column, in key order.
//
// Element types by family:
//   - FamilySigned, FamilyYear: int64
//   - FamilyUnsigned: uint64
//   - FamilyDecimal and the temporal families: string, the exact SQL text MySQL returned
//   - FamilyString, FamilyBinary: []byte, the exact bytes MySQL returned
//
// Go never orders keys (spec §4.2 rule 1) and compares them only byte for byte, for values
// read back from the same column (rule 2).
//
// A Key is immutable once built. Set, the log and every statement keep it, byte slices
// included, without copying (M1-D3); Decode is what copies a row's bytes.
type Key []any

// Validate reports whether k has one element of the right Go type per column of d.
func (d Def) Validate(k Key) error {
	if len(k) != len(d.Columns) {
		return fmt.Errorf("key: %s: key has %d values, the key has %d columns", d.Table, len(k), len(d.Columns))
	}
	for i, c := range d.Columns {
		ok := false
		switch c.Family {
		case FamilySigned, FamilyYear:
			_, ok = k[i].(int64)
		case FamilyUnsigned:
			_, ok = k[i].(uint64)
		case FamilyDecimal, FamilyDate, FamilyDateTime, FamilyTimestamp, FamilyTime:
			_, ok = k[i].(string)
		case FamilyString, FamilyBinary:
			_, ok = k[i].([]byte)
		}
		if !ok {
			return fmt.Errorf("key: %s.%s (%s) holds %T", d.Table, c.Name, c.Family, k[i])
		}
	}
	return nil
}

// Encode returns an injective byte encoding of k, used as a map key for de-duplication.
// Each element is a type tag, a length and the value's bytes, so ("ab","") and ("a","b")
// encode differently.
func Encode(k Key) string {
	buf := make([]byte, 0, 16*len(k))
	var n [binary.MaxVarintLen64]byte
	put := func(tag byte, b []byte) {
		buf = append(buf, tag)
		buf = append(buf, n[:binary.PutUvarint(n[:], uint64(len(b)))]...)
		buf = append(buf, b...)
	}
	for _, v := range k {
		switch x := v.(type) {
		case int64:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(x))
			put('i', b[:])
		case uint64:
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], x)
			put('u', b[:])
		case string:
			put('s', []byte(x))
		case []byte:
			put('b', x)
		default:
			put('?', []byte(fmt.Sprintf("%T:%v", v, v)))
		}
	}
	return string(buf)
}

// Set holds distinct keys in the order they were first added.
type Set struct {
	seen map[string]struct{}
	keys []Key
}

// NewSet returns an empty Set.
func NewSet() *Set { return &Set{seen: map[string]struct{}{}} }

// Add adds k and reports whether it was new.
func (s *Set) Add(k Key) bool {
	e := Encode(k)
	if _, dup := s.seen[e]; dup {
		return false
	}
	s.seen[e] = struct{}{}
	s.keys = append(s.keys, k)
	return true
}

// Has reports whether k is in the set.
func (s *Set) Has(k Key) bool { _, ok := s.seen[Encode(k)]; return ok }

// Keys returns the keys in insertion order. The caller must not modify the slice.
func (s *Set) Keys() []Key { return s.keys }

// Len returns the number of distinct keys.
func (s *Set) Len() int { return len(s.keys) }

// Decode converts one row of Projection(d), read as raw bytes, into a Key. It copies
// every byte slice, so raw may be sql.RawBytes reused by the next rows.Next.
func (d Def) Decode(raw [][]byte) (Key, error) {
	if len(raw) != len(d.Columns) {
		return nil, fmt.Errorf("key: %s: row has %d key values, the key has %d columns", d.Table, len(raw), len(d.Columns))
	}
	k := make(Key, len(raw))
	for i, c := range d.Columns {
		b := raw[i]
		if b == nil {
			return nil, fmt.Errorf("key: %s.%s is NULL; a primary-key column cannot be", d.Table, c.Name)
		}
		switch c.Family {
		case FamilySigned, FamilyYear:
			v, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("key: %s.%s: %w", d.Table, c.Name, err)
			}
			k[i] = v
		case FamilyUnsigned:
			v, err := strconv.ParseUint(string(b), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("key: %s.%s: %w", d.Table, c.Name, err)
			}
			k[i] = v
		case FamilyDecimal, FamilyTime:
			k[i] = string(b)
		case FamilyDate, FamilyDateTime, FamilyTimestamp:
			text := string(b)
			if err := types.ValidateTemporalKey(temporalKind(c.Family), text); err != nil {
				return nil, fmt.Errorf("key: %s.%s: %w", d.Table, c.Name, err)
			}
			k[i] = text
		case FamilyString, FamilyBinary:
			// Copy into a non-nil slice: an empty key ('' or an empty VARBINARY) is a value,
			// and go-sql-driver binds a nil []byte as SQL NULL, which would match nothing.
			v := make([]byte, len(b))
			copy(v, b)
			k[i] = v
		default:
			return nil, fmt.Errorf("key: %s.%s has unclassified family %s", d.Table, c.Name, c.Family)
		}
	}
	return k, nil
}

// Scan reads the current row of rows, whose select list is exactly Projection(d), into a Key.
func (d Def) Scan(rows *sql.Rows) (Key, error) {
	raw := make([]sql.RawBytes, len(d.Columns))
	dest := make([]any, len(raw))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}
	bs := make([][]byte, len(raw))
	for i := range raw {
		bs[i] = raw[i]
	}
	return d.Decode(bs)
}

func temporalKind(f Family) types.TemporalKind {
	switch f {
	case FamilyDate:
		return types.TemporalDate
	case FamilyDateTime:
		return types.TemporalDateTime
	case FamilyTimestamp:
		return types.TemporalTimestamp
	}
	return types.TemporalNone
}
