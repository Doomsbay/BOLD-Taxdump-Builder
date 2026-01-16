package cmd

import (
	"fmt"
	"os"
)

func Execute(args []string) {
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	switch args[0] {
	case "extract":
		runExtract(args[1:])
	case "markers":
		runMarkers(args[1:])
	case "package":
		runPackage(args[1:])
	case "pipeline":
		runPipeline(args[1:])
	case "-h", "--help", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", args[0])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "BoldKit - BOLD TSV processing tools")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  boldkit <command> [options]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Commands:")
	fmt.Fprintln(os.Stderr, "  extract    Build taxonkit_input.tsv")
	fmt.Fprintln(os.Stderr, "  markers    Build per-marker FASTA files")
	fmt.Fprintln(os.Stderr, "  package    Package release artifacts")
	fmt.Fprintln(os.Stderr, "  pipeline   Full pipeline: extract -> taxdump -> markers -> package (optional)")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Run 'boldkit <command> -h' for command-specific options.")
}
