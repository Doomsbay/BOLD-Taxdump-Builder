package cmd

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

const writerBufferSize = 1 << 20

func runExtract(args []string) {
	fs := flag.NewFlagSet("extract", flag.ExitOnError)
	input := fs.String("input", "BOLD_Public.*/BOLD_Public.*.tsv", "BOLD TSV input")
	output := fs.String("output", "taxonkit_input.tsv", "Output taxonkit input TSV")
	progressOn := fs.Bool("progress", true, "Show progress bar")
	force := fs.Bool("force", false, "Overwrite existing outputs")
	if err := fs.Parse(args); err != nil {
		fatalf("parse args failed: %v", err)
	}

	if !*force && fileExists(*output) {
		fmt.Fprintf(os.Stderr, "Output exists, skipping: %s\n", *output)
		return
	}

	totalRows := -1
	if *progressOn {
		count, err := countLines(*input)
		if err != nil {
			fatalf("count rows failed: %v", err)
		}
		if count > 0 {
			totalRows = count - 1
		}
	}

	reportEvery := 0
	if *progressOn {
		reportEvery = 1
	}

	if _, err := buildTaxonkit(*input, *output, reportEvery, totalRows); err != nil {
		fatalf("build failed: %v", err)
	}
}

func buildTaxonkit(inputPath, outputPath string, reportEvery, totalRows int) (int, error) {
	in, err := openInput(inputPath)
	if err != nil {
		return 0, fmt.Errorf("open input: %w", err)
	}
	defer func() {
		_ = in.Close()
	}()

	out, err := os.Create(outputPath)
	if err != nil {
		return 0, fmt.Errorf("create output: %w", err)
	}
	defer func() {
		_ = out.Close()
	}()

	writer := bufio.NewWriterSize(out, writerBufferSize)
	defer func() {
		_ = writer.Flush()
	}()

	scanner := bufio.NewScanner(in)
	buf := make([]byte, 0, 1024*1024)
	scanner.Buffer(buf, 50*1024*1024)

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return 0, fmt.Errorf("read header: %w", err)
		}
		return 0, errors.New("input TSV is empty")
	}

	header := strings.Split(scanner.Text(), "\t")
	idxProcess := indexOf(header, "processid")
	idxBin := indexOf(header, "bin_uri")
	idxKingdom := indexOf(header, "kingdom")
	idxPhylum := indexOf(header, "phylum")
	idxClass := indexOf(header, "class")
	idxOrder := indexOf(header, "order")
	idxFamily := indexOf(header, "family")
	idxSubfamily := indexOf(header, "subfamily")
	idxTribe := indexOf(header, "tribe")
	idxGenus := indexOf(header, "genus")
	idxSpecies := indexOf(header, "species")
	if idxProcess < 0 || idxBin < 0 || idxKingdom < 0 || idxPhylum < 0 || idxClass < 0 ||
		idxOrder < 0 || idxFamily < 0 || idxSubfamily < 0 || idxTribe < 0 || idxGenus < 0 ||
		idxSpecies < 0 {
		return 0, errors.New("required headers missing in input TSV")
	}

	if _, err := writer.WriteString("kingdom\tphylum\tclass\torder\tfamily\tsubfamily\ttribe\tgenus\tspecies\tprocessid\n"); err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}

	progress := newProgress(totalRows, reportEvery)
	var rowCount int
	for scanner.Scan() {
		rowCount++
		fields := strings.Split(scanner.Text(), "\t")

		pid := field(fields, idxProcess)
		bin := field(fields, idxBin)
		kingdom := normalize(field(fields, idxKingdom))
		phylum := normalize(field(fields, idxPhylum))
		classVal := normalize(field(fields, idxClass))
		orderVal := normalize(field(fields, idxOrder))
		family := normalize(field(fields, idxFamily))
		subfamily := normalize(field(fields, idxSubfamily))
		tribe := normalize(field(fields, idxTribe))
		genus := normalize(field(fields, idxGenus))
		species := normalize(field(fields, idxSpecies))

		if genus != "" && species == "" {
			suffix := bin
			if suffix == "" {
				suffix = pid
			}
			species = genus + " sp. " + suffix
		}

		line := strings.Join([]string{
			kingdom, phylum, classVal, orderVal, family, subfamily, tribe, genus, species, pid,
		}, "\t")
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return 0, fmt.Errorf("write row: %w", err)
		}

		progress.increment()
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scan input: %w", err)
	}

	progress.finish()
	return rowCount, nil
}
