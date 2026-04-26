package cmd

import (
	"path/filepath"
	"strings"
)

func isParquetPath(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	return ext == ".parquet" || ext == ".parq"
}

func ParseRows(path string, opts Options, onRow func(Row) error) error {
	if isParquetPath(path) {
		return parseParquet(path, opts, onRow)
	}
	return parseTSVRows(path, opts, onRow)
}

func RowCount(path string) (int64, error) {
	if isParquetPath(path) {
		return parquetRowCount(path)
	}
	n, err := countLines(path)
	if err != nil {
		return 0, err
	}
	if n > 0 {
		return int64(n - 1), nil
	}
	return 0, nil
}

func InputFormat(path string) string {
	if isParquetPath(path) {
		return "parquet"
	}
	return "tsv"
}
