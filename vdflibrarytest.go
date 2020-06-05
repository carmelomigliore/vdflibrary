package main

import (
	"log"


	"github.com/mediocregopher/radix"
	"github.com/carmelomigliore/vdflibrary"
)



func main() {
 
	client, err := radix.NewPool("tcp", "127.0.0.1:6379", 10) // or any other client
	if err != nil {
		// handle error
		log.Println("Error: ", err)
	}

	vdflibrary.InitREDISQLDB(client, "DATABASE", "files_to_read", "FILE_PATH", "FILE_CREATION_TIMESTAMP", "FILE_READ", "FILE_READ_TIMESTAMP", "/home/migelankodra/Documents/GoTest")

	pathOfFiles := vdflibrary.GetPathsFromDB(client, "DATABASE", "files_to_read", "FILE_PATH")
	if len(pathOfFiles) == 0 {
		log.Println("There are no file paths saved in DB")
	} else {
		log.Println("Here is the list of the file paths currently saved in DB")
		log.Println(pathOfFiles)
	}

}