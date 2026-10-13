package engine

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// longData is the server's refusal of one value larger than its max_allowed_packet, sent as
// long data (probed on 8.0.46, 8.4.11 and 9.7.1, M7c-F1).
var longData = &mysql.MySQLError{Number: 1105, SQLState: [5]byte{'H', 'Y', '0', '0', '0'},
	Message: "Parameter of prepared statement which is set through mysql_send_long_data() is longer than 'max_allowed_packet' bytes"}

// TestPacketError (M7c-F1): an INSERT of the copy that does not fit the destination's
// max_allowed_packet is named by table, and by column and size when one value is too
// large; any other error is not.
func TestPacketError(t *testing.T) {
	cols := []string{"doc_id", "body", "title"}
	rows := [][]any{
		{int64(1), []byte(strings.Repeat("a", 10)), "short"},
		{int64(2), []byte(strings.Repeat("b", 30)), strings.Repeat("t", 20)},
		{int64(3), nil, strings.Repeat("t", 40)},
	}
	for _, tc := range []struct {
		name string
		err  error
		want *PacketError
	}{
		{"one value", fmt.Errorf("wrapped: %w", longData), &PacketError{Table: "pages", Column: "title", Bytes: 40}},
		{"the driver's limit", fmt.Errorf("wrapped: %w", mysql.ErrPktTooLarge), &PacketError{Table: "pages"}},
		{"the server's 1153", &mysql.MySQLError{Number: 1153, Message: "Got a packet bigger than 'max_allowed_packet' bytes"}, &PacketError{Table: "pages"}},
		{"another 1105", &mysql.MySQLError{Number: 1105, Message: "Unknown error"}, nil},
		{"another error", errors.New("boom"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := packetError("pages", cols, rows, tc.err)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("packetError = %+v, want nil", got)
				}
				return
			}
			if got == nil || *got != *tc.want {
				t.Fatalf("packetError = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPacketErrorMessage(t *testing.T) {
	for _, tc := range []struct {
		err  *PacketError
		want []string
	}{
		{&PacketError{Table: "pages", Column: "body", Bytes: 5242880},
			[]string{"a value of 5242880 bytes in pages.body", "larger than the destination's max_allowed_packet",
				"nothing was logged or deleted", "Raise max_allowed_packet on the destination above 5242880 bytes"}},
		{&PacketError{Table: "pages"},
			[]string{"an INSERT into pages", "larger than the destination's max_allowed_packet",
				"nothing was logged or deleted", "Raise max_allowed_packet on the destination", "lower batch_size"}},
	} {
		msg := tc.err.Error()
		for _, w := range tc.want {
			if !strings.Contains(msg, w) {
				t.Errorf("%+v: message %q lacks %q", *tc.err, msg, w)
			}
		}
	}
}
