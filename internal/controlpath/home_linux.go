package controlpath

import (
	"bufio"
	"errors"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
)

// lookupHome reads /etc/passwd for uid. A missing file is a database without the entry; an
// account that only a directory service knows has no entry here either, and gets no default.
func lookupHome(uid int) (string, bool, error) {
	f, err := os.Open("/etc/passwd")
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	return homeFromPasswd(f, uid)
}

// homeFromPasswd returns the home field of the first entry for uid, as getpwuid does. Blank
// lines, comments and lines that are not entries (too few fields, a uid that is not a
// number, a NIS "+" line) are skipped. found is false when no entry matches.
func homeFromPasswd(r io.Reader, uid int) (string, bool, error) {
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadString('\n')
		if home, ok := passwdHome(line, uid); ok {
			return home, true, nil
		}
		if err == io.EOF {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
	}
}

// passwdHome parses name:passwd:uid:gid:gecos:home:shell; the shell may be absent.
func passwdHome(line string, uid int) (string, bool) {
	line = strings.TrimRight(line, "\r\n")
	if line == "" || strings.HasPrefix(line, "#") {
		return "", false
	}
	fields := strings.Split(line, ":")
	if len(fields) < 6 {
		return "", false
	}
	id, err := strconv.Atoi(fields[2])
	if err != nil || id != uid {
		return "", false
	}
	return fields[5], true
}
