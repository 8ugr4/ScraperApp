package main

import "fmt"
// fixed. 
// now make this into a real dnsResolver or something else. 	
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
func (d dnsResolver) control(){
	fmt.Println("checking dnsResolver")
	d.files.control()
}
