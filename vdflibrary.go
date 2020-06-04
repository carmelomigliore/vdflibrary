package vdflibrary

import(
	"log"
	"os"
	"bufio"
	"net/http"
	"sync"
)

var fileNames = [3]string{"/mnt/app/datalog.dat"}
var scanner *bufio.Scanner
var err error 
var file *os.File
var mutex = &sync.Mutex{}
var state bool 

func Length() int {
	return len(fileNames)
}

func Initial (x int) *bufio.Scanner {

	file, err = os.Open(fileNames[x])
 
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
		os.Exit(-1)
	} else {
		scanner = bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
	}
	return scanner
}

func Stop() {
	file.Close()
}

func StateSet(x bool) {
	mutex.Lock()
	state = x
	mutex.Unlock()
}

func StateGet() bool {
	mutex.Lock()
	value := state
	mutex.Unlock()
	return value
}

//da runnare come una goroutine dentro al driver
func HealthCheck() int {
	var OK = 0
	
	//try to ping core-data
	_, err := http.Get("http://localhost:48080/api/vi/ping")
	if err != nil {
		log.Fatal(err)
		OK = 1
	}
	//try to ping core-metadata
	_, err = http.Get("http://localhost:48081/api/v1/ping")
	if err != nil {
		log.Fatal(err)
		OK = 1
	}
	//check if parser set state to true
	state := StateGet()
	if state == false {
		OK = 1
	}


	return OK
}
