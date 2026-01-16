package main

import (
	"os"

	"github.com/Doomsbay/BoldKit/boldkit/cmd"
)

func main() {
	cmd.Execute(os.Args[1:])
}
