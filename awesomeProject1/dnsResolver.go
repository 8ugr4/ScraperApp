package main

import (
	"fmt"
	"io/fs"
)

type directory interface {
	createNewDirectory(fsys fs.FS, outputPath string) error
}

type dnsResolver struct {
	files
	directory
	chTransfer
}

// STILL GOTTA IMPLEMENT THIS FILE.
func (d dnsResolver) Check() {
	fmt.Println("Checking")
}
func (d dnsResolver) control() {
	fmt.Println("dnsResolver control")
	//d.files.control()
}
