package utils

import (
	"bufio"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
)

func GetHashFromStr(str string) int {
	v := int(crc32.ChecksumIEEE([]byte(str)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	return 0
}

func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

func IsFile(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !s.IsDir()
}

func Str2Int(str string) int {
	v, e := strconv.ParseInt(str, 10, 64)
	if e == nil {
		return int(v)
	}
	return 0
}

var CommonHandler = func(line string) string {
	line = strings.Trim(line, "")
	line = strings.ReplaceAll(line, "\n", "")
	return line
}

func File2list(filepath string, handler func(line string) string) ([]string, error) {
	var lines []string
	fi, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(fi)
	for {
		line, err := br.ReadString('\n')
		record := line
		record = handler(record)
		lines = append(lines, record)
		if err == io.EOF {
			break
		}
	}
	return lines, nil
}
