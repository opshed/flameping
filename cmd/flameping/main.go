package main

import (
	"context"
	"os"

	"github.com/opshed/flameping/internal/cli"
)

func main() { os.Exit(cli.Execute(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }
