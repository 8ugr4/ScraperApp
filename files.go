package main

import "fmt"

type files struct { //engine
	a string
}

func (f files) control() {
	fmt.Println("file controlled.")
}
