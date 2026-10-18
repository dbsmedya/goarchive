package preflight

import (
	"context"
	"slices"
	"strings"

	"github.com/dbsmedya/dbsgomysql/pkg/validations"
)

// supportedCharsets are the character sets whose every character survives GoArchive's
// utf8mb4 session exactly: read as utf8mb4, distinct, and bound back to the same bytes
// (CHS-1, spec §1). The other 22 lose data: two characters read as one (sjis, cp932,
// eucjpms, ujis, armscii8), a code reads as '?' or U+FFFD (cp1250, cp1251, cp1256, cp1257,
// dec8, geostd8, greek, hebrew, hp8, swe7, big5, euckr, gb2312, gbk, gb18030, tis620), or a
// lone surrogate reads as nothing (ucs2). TestMySQLCharsetRoundTrip proves the list on every
// supported server. docs/README_CHARSETS.md shows it, and the harness reads this literal.
var supportedCharsets = []string{
	"utf8mb4",
	"utf8mb3",
	"ascii",
	"latin1",
	"latin2",
	"latin5",
	"latin7",
	"cp850",
	"cp852",
	"cp866",
	"keybcs2",
	"koi8r",
	"koi8u",
	"macce",
	"macroman",
	"utf16",
	"utf16le",
	"utf32",
	"binary",
}

// checkColumnCharsets refuses a job column the run reads or writes as text in a character set
// outside supportedCharsets (COLUMN_CHARSET_CHECK). Archive and copy-only read every source
// column and write, and verify, the destination's columns of the same name; purge reads and
// binds only primary keys and foreign_key columns (references names the parent's key). A
// column with no character set (numbers, temporal types, binary strings, JSON) is not text.
func checkColumnCharsets(ctx context.Context, f *facts) error {
	src, err := f.sourceSpecs(ctx)
	if err != nil {
		return inspectionError("column_charsets", err)
	}
	purge := ProfileFor(f.cfg.Spec.Type) == ProfileSourceOnly
	var dst map[string]validations.TableSpec
	if !purge {
		if dst, err = f.destSpecs(ctx); err != nil {
			return inspectionError("column_charsets", err)
		}
	}
	unsupported := func(c validations.ColumnSpec) bool {
		return c.Charset != "" && !slices.Contains(supportedCharsets, c.Charset)
	}
	var offenders []string
	for _, t := range f.tables {
		var read []string // purge's columns; nil reads every column
		if purge {
			read = append(slices.Clone(f.keys[t].Names()), f.tree.Node(t).ForeignKey...)
		}
		for _, c := range src[t].Columns {
			if read != nil && !slices.ContainsFunc(read, func(n string) bool { return strings.EqualFold(n, c.Name) }) {
				continue
			}
			if unsupported(c) {
				offenders = append(offenders, t+"."+c.Name+"("+c.Charset+")")
			}
		}
		if purge {
			continue
		}
		for _, c := range dst[t].Columns {
			copied := slices.ContainsFunc(src[t].Columns, func(s validations.ColumnSpec) bool { return strings.EqualFold(s.Name, c.Name) })
			if copied && unsupported(c) {
				offenders = append(offenders, "destination:"+t+"."+c.Name+"("+c.Charset+")")
			}
		}
	}
	if len(offenders) > 0 {
		return &Error{Check: "COLUMN_CHARSET_CHECK", Tables: offenders,
			Message: "A job column uses a character set that GoArchive cannot read and write back exactly through its utf8mb4 session, so keys could merge and text change without notice"}
	}
	return nil
}
