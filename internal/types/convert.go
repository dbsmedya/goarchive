package types

import (
	"fmt"
	"strconv"
	"strings"
)

// ConvertRootPK converts a string-stored root PK back to a numeric driver value.
func ConvertRootPK(raw, dataType string, unsigned bool) (interface{}, error) {
	if raw == "" {
		return nil, fmt.Errorf("empty root PK string")
	}
	switch strings.ToLower(dataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		if unsigned {
			v, err := strconv.ParseUint(raw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("ConvertRootPK: parse uint %q: %w", raw, err)
			}
			return v, nil
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ConvertRootPK: parse int %q: %w", raw, err)
		}
		return v, nil
	default:
		return nil, fmt.Errorf("ConvertRootPK: unsupported root PK type %q (only integer types supported)", dataType)
	}
}
