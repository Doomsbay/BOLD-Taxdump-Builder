package cmd

import (
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

func parseParquet(path string, opts Options, onRow func(Row) error) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open parquet %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	pf, err := parquet.OpenFile(f, info.Size(),
		parquet.SkipBloomFilters(true),
		parquet.SkipPageIndex(true),
	)
	if err != nil {
		return fmt.Errorf("open parquet file: %w", err)
	}

	schema := pf.Schema()
	schemaFields := schema.Fields()
	numFields := len(schemaFields)

	header := make([][]byte, numFields)
	for i := 0; i < numFields; i++ {
		header[i] = []byte(schemaFields[i].Name())
	}
	if err := onRow(Row{Line: 0, Fields: header}); err != nil {
		return err
	}

	rowBuf := make([]parquet.Row, 1024)
	lineNum := int64(0)

	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()

		for {
			n, err := rows.ReadRows(rowBuf)
			if err != nil && err != io.EOF {
				rows.Close()
				return fmt.Errorf("read parquet rows: %w", err)
			}

			for _, parquetRow := range rowBuf[:n] {
				lineNum++
				fields := make([][]byte, numFields)
				for j := 0; j < numFields && j < len(parquetRow); j++ {
					val := parquetRow[j]
					if val.IsNull() {
						fields[j] = nil
						continue
					}
					fields[j] = []byte(val.String())
				}
				if opts.Progress != nil {
					if !opts.SkipProgressFirstRow || lineNum != 1 {
						opts.Progress.increment()
					}
				}
				if err := onRow(Row{Line: lineNum, Fields: fields}); err != nil {
					rows.Close()
					return err
				}
			}

			if err == io.EOF {
				break
			}
		}

		rows.Close()
	}

	return nil
}

func parquetRowCount(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}

	pf, err := parquet.OpenFile(f, info.Size(),
		parquet.SkipBloomFilters(true),
		parquet.SkipPageIndex(true),
	)
	if err != nil {
		return 0, fmt.Errorf("open parquet file: %w", err)
	}

	return pf.NumRows(), nil
}
