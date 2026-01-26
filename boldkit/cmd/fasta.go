package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
)

type fastaRecord struct {
	id   string
	seq  []byte
}

func parseFasta(r io.Reader, onRecord func(fastaRecord) error) error {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)

	var header string
	var seq bytes.Buffer
	emit := func() error {
		if header == "" {
			return nil
		}
		rec := fastaRecord{
			id:  fastaID(header),
			seq: append([]byte(nil), seq.Bytes()...),
		}
		seq.Reset()
		header = ""
		return onRecord(rec)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, ">") {
			if err := emit(); err != nil {
				return err
			}
			header = strings.TrimSpace(line[1:])
			continue
		}
		seq.WriteString(strings.TrimSpace(line))
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan fasta: %w", err)
	}
	if err := emit(); err != nil {
		return err
	}
	return nil
}

func fastaID(header string) string {
	if header == "" {
		return ""
	}
	fields := strings.Fields(header)
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func sanitizeTaxon(name string) string {
	if name == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z':
			b.WriteByte(c)
		case c >= 'a' && c <= 'z':
			b.WriteByte(c)
		case c >= '0' && c <= '9':
			b.WriteByte(c)
		case c == '-' || c == '_' || c == '.':
			b.WriteByte(c)
		case c == ' ':
			b.WriteByte('_')
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
