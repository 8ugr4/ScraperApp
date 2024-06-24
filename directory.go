package main

// deals with directory operations
import (
	"fmt"
	"io/fs"
	"log"
	"os"
)

type Directory struct {
	outputPath string
}

func check(e error) {
	if e != nil {
		fmt.Println("err:", e)
		panic(e)
	}
}

// using os module creates a new directory.
func (d Directory) create() {
	err := os.Mkdir(d.outputPath, 0755)
	check(err)

	defer os.RemoveAll("sub")
}

// Checks the name of the directory to be true or false
func (d Directory) checkName(fsys fs.FS, name string) ([]byte, error) {
	if fs.ValidPath(name) == true {
		return fs.ReadFile(fsys, name)
	}
	_, err := fs.Stat(fsys, name)
	if err != nil {
		return nil, err
	}
	return nil, err
}

// Calls d.create() to create a new directory.
func (d Directory) createNewDirectory(fsys fs.FS, outputPath string) {
	fmt.Println("Creating new directory")
	var name string = "newDirectory"
	//fsys check later
	_, err := d.checkName(fsys, name)
	if err != nil {
		log.Fatalf("err", err)
	}
	d.create()

}
