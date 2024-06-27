package main

import "fmt"

// fixed.
// now implement actual file control in this one.
type files struct { //engine
	a string
}

func (f files) control() {
	fmt.Println("file controlled.")
}
