package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type parquetTestRow struct {
	ProcessID  string `parquet:"processid"`
	MarkerCode string `parquet:"marker_code"`
	Nuc        string `parquet:"nuc"`
}

func TestParquetRowCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.parquet")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	rows := make([]parquetTestRow, 100)
	for i := range rows {
		rows[i] = parquetTestRow{
			ProcessID:  "TEST001",
			MarkerCode: "COI-5P",
			Nuc:        "ACGTACGT",
		}
	}

	if err := parquet.Write(f, rows); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	_ = f.Close()

	count, err := parquetRowCount(path)
	if err != nil {
		t.Fatal(err)
	}
	if count != 100 {
		t.Errorf("expected 100 rows, got %d", count)
	}
}

func TestIsParquetPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"data.parquet", true},
		{"data.PARQUET", true},
		{"data.parq", true},
		{"data.PARQ", true},
		{"data.tsv", false},
		{"data.tsv.gz", false},
		{"data.csv", false},
	}
	for _, tt := range tests {
		got := isParquetPath(tt.path)
		if got != tt.want {
			t.Errorf("isParquetPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}
