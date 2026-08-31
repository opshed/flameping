package webui

import "embed"

// Dist is committed so production builds do not require a JavaScript toolchain.
//
//go:embed dist/*
var Dist embed.FS
