package main

import "fmt"
//NOT FIXED
type directory interface { //transmission
	createNewDirectory()
}

type dnsResolver struct { //convertable
	files     //engine
	directory //transmission
}

func (d dnsResolver) Check() { //convertable
	fmt.Println("Checking")
}
