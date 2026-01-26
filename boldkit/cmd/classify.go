package cmd

import (
	"flag"
	"fmt"
	"path/filepath"
	"strings"
)

func runClassify(args []string) {
	fs := flag.NewFlagSet("classify", flag.ExitOnError)
	input := fs.String("input", "", "Input FASTA/FASTA.gz")
	outDir := fs.String("outdir", "classifier_outputs", "Output directory")
	classifiers := fs.String("classifier", "blast", "Comma-separated classifiers")
	taxdumpDir := fs.String("taxdump-dir", "bold-taxdump", "Taxdump directory with nodes.dmp/names.dmp/taxid.map")
	taxidMap := fs.String("taxid-map", "", "Optional taxid.map override")
	requireRanks := fs.String("require-ranks", "kingdom,phylum,class,order,family,genus,species", "Comma-separated ranks required to keep a sequence (empty disables)")
	qcMin := fs.Int("qc-min-length", 200, "QC minimum cleaned length")
	qcMax := fs.Int("qc-max-length", 700, "QC maximum cleaned length")
	qcMaxN := fs.Int("qc-max-n", 0, "QC maximum N count")
	qcMaxAmbig := fs.Int("qc-max-ambig", 0, "QC maximum IUPAC ambiguous count")
	qcMaxInvalid := fs.Int("qc-max-invalid", 0, "QC maximum invalid character count")
	qcDedupe := fs.Bool("qc-dedupe", true, "QC drop duplicate sequences")
	qcDedupeIDs := fs.Bool("qc-dedupe-ids", true, "QC drop duplicate IDs")
	qcProgress := fs.Bool("qc-progress", true, "Show QC progress bar (approximate)")
	formatProgress := fs.Bool("format-progress", true, "Show format progress bar (approximate)")
	qcOnly := fs.Bool("qc-only", false, "Run QC only (skip classifier formatting)")
	compress := fs.Bool("compress", false, "Compress classifier output directories (.tar.gz)")
	force := fs.Bool("force", false, "Overwrite existing archives")
	if err := fs.Parse(args); err != nil {
		fatalf("parse args failed: %v", err)
	}

	if *input == "" {
		fatalf("input is required")
	}

	ranks := splitList(*requireRanks)
	classifierList := splitList(*classifiers)
	if len(classifierList) == 0 {
		fatalf("classifier must not be empty")
	}

	base := qcBaseName(*input)
	qcOut := filepath.Join(*outDir, "qc", base+".fasta")
	qcCfg := qcConfig{
		MinLen:       *qcMin,
		MaxLen:       *qcMax,
		MaxN:         *qcMaxN,
		MaxAmbig:     *qcMaxAmbig,
		MaxInvalid:   *qcMaxInvalid,
		DedupeSeqs:   *qcDedupe,
		DedupeIDs:    *qcDedupeIDs,
		RequireRanks: ranks,
		TaxdumpDir:   *taxdumpDir,
		TaxidMapPath: *taxidMap,
		OutputPath:   qcOut,
		Progress:     *qcProgress,
	}

	logf("QC -> %s", qcOut)
	if err := qcFasta(*input, qcCfg); err != nil {
		fatalf("qc failed: %v", err)
	}

	if *qcOnly {
		return
	}

	for _, classifier := range classifierList {
		if classifier == "" {
			continue
		}
		name := strings.ToLower(classifier)
		outPath := filepath.Join(*outDir, name)
		cfg := formatConfig{
			Classifiers:  []string{name},
			RequireRanks: ranks,
			Input:        qcOut,
			OutDir:       outPath,
			TaxdumpDir:   *taxdumpDir,
			TaxidMapPath: *taxidMap,
			Progress:     *formatProgress,
		}
		logf("Format %s -> %s", name, outPath)
		if err := formatFasta(cfg); err != nil {
			fatalf("format %s failed: %v", name, err)
		}

		if *compress {
			archive := filepath.Join(*outDir, name+".tar.gz")
			if err := packageDirGzip(outPath, archive, *force); err != nil {
				fatalf("compress %s failed: %v", name, err)
			}
		}
	}
}

func qcBaseName(path string) string {
	base := filepath.Base(path)
	if strings.HasSuffix(base, ".gz") {
		base = strings.TrimSuffix(base, ".gz")
	}
	ext := filepath.Ext(base)
	if ext != "" {
		base = strings.TrimSuffix(base, ext)
	}
	if base == "" {
		return "qc_output"
	}
	return base
}
