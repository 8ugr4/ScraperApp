package main

import "fmt"
//NOT FIXED
type files struct { //engine
	a string
}

func (f files) control() {
	fmt.Println("file controlled.")
}
