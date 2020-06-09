package vdflibrary

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	//	"reflect"

	"github.com/mediocregopher/radix/v3"
)

var fileNames []string
var scanner *bufio.Scanner
var reader *bufio.Reader
var err error
var file *os.File
var mutex = &sync.Mutex{}
var state bool

//Length function returns the array file names number of elements

func Length() int {
	fileNames = GetDirFilePaths(os.Getenv("DirectoryPath"))
	return len(fileNames)
}

//Initial function is used to open the file, taking is name from an array,
//and to create a new scanner on the new opened file
/*
* @params	x	index of the array for the name of the file
*
* This function returns the Scanner created on the new opened file
 */

func Initial(x int) *bufio.Scanner {

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

func InitReader(x int) *bufio.Reader {

	file, err = os.Open(fileNames[x])
	fmt.Println(fileNames[x])
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
		os.Exit(-1)
	} else {
		reader = bufio.NewReader(file)
	}
	return reader
}

//Stop function is used to close the file

func Stop() {
	file.Close()
}

//StateSet function is used to set the state to a defined value

/*
* @params	x	value to be assigned to state variable
*
 */

func StateSet(x bool) {
	mutex.Lock()
	state = x
	mutex.Unlock()
}

//StateGet function is used to check the state

func StateGet() bool {
	mutex.Lock()
	value := state
	mutex.Unlock()
	return value
}

//HealtCheck is used to verify if core-data and core-metadata are running and
//if the parser did not set the state variable to false

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

// CreateNewDB function is used to create a db

/*
 * @params	r				redisql client
 * @params	databaseName	name of the new db
 *
 */

func CreateNewDB(r radix.Client, databaseName string) {
	err := r.Do(radix.Cmd(nil, "REDISQL.CREATE_DB", databaseName))
	if err != nil {
		log.Fatal(err)
	}
}

// CreateDBTable function is used to create a table in the DB if it doesn't already exists

/*
 *
 * 	@params		r 				redisql client
 * 	@params		databaseName 	name of the new db
 *	@params		tableName 		name of the table
 *	@params		column1 		name of the first column - The first column is also the primry key
 *	@params		column2			name of the second column
 *	@params		column3			name of the third column
 *	@params		column4			name of the forth column
 *
 *	This function returns true if there aren't any errors, otherwise false
 *
 */

func CreateDBTable(r radix.Client, databaseName string, tableName string, column1 string, column2 string, column3 string, column4 string) bool {

	rediSQLcommand := "CREATE TABLE IF NOT EXISTS " + tableName + "(" + column1 + " TEXT PRIMARY KEY, " + column2 + " TEXT, " + column3 + " TEXT, " + column4 + " TEXT" + ");"
	log.Println("RediSQL Command: ", rediSQLcommand)

	err := r.Do(radix.Cmd(nil, "REDISQL.EXEC", databaseName, rediSQLcommand))
	if err != nil {
		log.Println(err)
		return false
	} else {
		return true
	}
}

// InsertValuesToDBTable function is used to insert values in the specified table

/*
 * 	@params		r 				redisql client
 * 	@params		databaseName 	name of the db
 *	@params		tableName 		name of the table
 *	@params		column1Value 	name of the value that goes in the first column
 *	@params		column2Value	name of the value that goes in the second column
 *	@params		column3Value	name of the value that goes in the third column
 *	@params		column4Value	name of the value that goes in the forth column
 *
 *	This function returns true if there aren't any errors, otherwise false
 *
 */

func InsertValuesToDBTable(r radix.Client, databaseName string, tableName string, column1Value string, column2Value string, column3Value string, column4Value string) bool {

	InsertElementsToDB := "INSERT INTO " + tableName + " VALUES" + "(" + "'" + column1Value + "'" + "," + "'" + column2Value + "'" + "," + "'" + column3Value + "'" + "," + "'" + column4Value + "'" + ");"
	err := r.Do(radix.Cmd(nil, "REDISQL.EXEC", databaseName, InsertElementsToDB))
	if err != nil {
		log.Println(err)
		return false
	}
	return true

}

// visit is a function that reads the name of files in a given directory

func visit(files *[]string) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println(err)
		}
		// if found a directory return nil
		if info.IsDir() {
			return nil
		}

		// skip files of a specific format -- this should be modifided accordingly
		if filepath.Ext(path) == ".sh" {
			return nil
		}

		// Get the file creation timestamp
		timeStamp := info.ModTime()
		// Convert the time (format time.Times) to string
		StringTs := timeStamp.String()
		// Format the information separated by "," [path/of/the/file, timestamp]
		fData := path + "," + StringTs

		// Save the fData into the structure *files
		*files = append(*files, fData)
		return nil
	}
}

// GetFilePaths is a function that will read the specified directory and return
// an array where are saved the files present in that directory

/*
 *
 * 	@params		directoryPath	path of the directory where to read filenames from
 *
 */

func GetDirFilePaths(directoryPath string) (pathofElements []string) {

	// Array files of type string to save the files in the format: [path/of/the/file, timestamp]
	var files []string
	var records [][]string

	fmt.Println("Directory path:", directoryPath)
	// Read all files in the directoryPath
	err := filepath.Walk(directoryPath, visit(&files))
	if err != nil {
		log.Println(err)
	}

	// define a variable dirFilePath to save the path values
	var dirFilePath []string

	// scan all the files
	for z := 0; z < len(files); z = z + 1 {

		// assing to the variable filetoread the z-th file
		filetoread := files[z]

		// use csv module, to read the [path/of/the/file, timestamp]
		readVector := csv.NewReader(strings.NewReader(filetoread))

		//save the elements in the records
		records, err = readVector.ReadAll()
		if err != nil {
			log.Println(err)
		}

		// save file path in append
		dirFilePath = append(dirFilePath, records[0][0])
	}

	// return an array with file paths: [path1 path2 path3 ... pathN]
	return (dirFilePath)

}

// InitInsertPathsInDB function is used for the initial path insertion in db

/*
 * 	@params		r 							redisql client
 * 	@params		databaseName 				name of the db
 *	@params		tableName 					name of the table
 *	@params		pathVector 					a vector that holds file paths [path1, path2 ... pathN]
 *	@params		fileCreatiionTimeStamp		Holds the timestamp when the file was created
 *	@params		readFlag					holds the information if the file is alread read or not
 *	@params		readingTimeStamp			holds the timestamp when the reading was done
 *
 *
 */

func InitInsertPathsInDB(r radix.Client, databaseName string, tableName string, pathVector []string, fileCreationTimeStamp string, readFlag string, readingTimeStamp string) {

	if len(pathVector) < 0 {
		log.Println("Error:", pathVector, "is empty")
	} else {
		for i := 0; i < len(pathVector); i = i + 1 {
			InsertElementsToDB := "INSERT INTO " + tableName + " VALUES" + "(" + "'" + pathVector[i] + "'" + "," + "'" + fileCreationTimeStamp + "'" + "," + "'" + readFlag + "'" + "," + "'" + readingTimeStamp + "'" + ");"
			err := r.Do(radix.Cmd(nil, "REDISQL.EXEC", databaseName, InsertElementsToDB))
			if err != nil {
				log.Println(err)
			}
		}
	}
}

// TableExists function is used to check if the table is already created

/*
 * 	@params		r 				redisql client
 * 	@params		databaseName 	name of the db
 *	@params		tableName 		name of the table
 *
 *	This function returns true if the table exists, otherwise false
 *
 */

func TableExists(r radix.Client, databaseName string, tableName string) bool {

	var count [][]int
	countCommand := "SELECT COUNT(*) FROM " + tableName + ";"
	message := r.Do(radix.Cmd(&count, "REDISQL.EXEC", databaseName, countCommand))
	log.Println("Message Error:", message)

	if len(count) == 0 {
		log.Println("Table does not exist")
		return false
	} else {
		log.Println("Table exists .. Number of elements: ", count[0][0])
		return true
	}

}

// GetPathsFromDB function is used to get all the paths already saved in db

/*
 * 	@params		r 					redisql client
 * 	@params		databaseName 		name of the db
 *	@params		tableName 			name of the table
 *	@params		filePathColumn 		name of the column where the file path values are saved
 *
 *	This function returns true if the table exists, otherwise false
 *
 */

func GetPathsFromDB(r radix.Client, databaseName string, tableName string, filePathColumn string) (DBfilePathArray []string) {

	var FilesAlreadySavedInDB [][]string
	getFilePathFromDBcommand := "SELECT " + filePathColumn + " FROM " + tableName + ";"
	log.Println(getFilePathFromDBcommand)
	r.Do(radix.Cmd(&FilesAlreadySavedInDB, "REDISQL.EXEC", databaseName, getFilePathFromDBcommand))

	var FilesAlreadySavedInDBArray []string

	for i := 0; i < len(FilesAlreadySavedInDB); i = i + 1 {
		FilesAlreadySavedInDBArray = append(FilesAlreadySavedInDBArray, FilesAlreadySavedInDB[i][0])
	}

	return FilesAlreadySavedInDBArray
}

// difference is the function that makes the difference between two arrays
func difference(a, b []string) []string {
	target := map[string]bool{}
	for _, x := range b {
		target[x] = true
	}

	result := []string{}
	for _, x := range a {
		if _, ok := target[x]; !ok {
			result = append(result, x)
		}
	}
	return result
}

// GetPathsOfNewFiles function makes the difference between the vector
// of Directory Files path and the vector of file paths already saved in db
// The function returns an array of the new files present on the directory, if any

func GetPathsOfNewFiles(currentFilesinDir []string, currentFilesinDB []string) (NewFiles []string) {
	NewFiles = difference(currentFilesinDir, currentFilesinDB)
	if len(NewFiles) == 0 {
		log.Println("There are no new files")
	}
	return NewFiles
}

// GetPathOfFilesToReadFromDB function returns an array
// of files still to be read (read flag is set to false)
/*
 * 	@params		r 					redisql client
 * 	@params		databaseName 		name of the db
 *	@params		tableName 			name of the table
 *	@params		filePathColumn 		name of the column where the file path values are saved
 *	@params		readFlag			Will be False if the file is not been read yet, otherwise True
 *
 *	This function returns an array that holds the paths of files with readFlag = False (files to be read)
 *
 */

func GetPathOfFilesToReadFromDB(r radix.Client, databaseName string, tableName string, filePathColumn string, readFlag string) (filesToRead []string) {

	var ElementsToRead [][]string
	Command := "SELECT " + filePathColumn + " FROM " + tableName + " WHERE " + readFlag + " = " + "'False'"
	err := r.Do(radix.Cmd(&ElementsToRead, "REDISQL.EXEC", databaseName, Command))
	if err != nil {
		log.Println(err)
	}

	var ElementsToReadArray []string

	if len(ElementsToRead[0]) == 0 {
		log.Println("Currently there are no files to read in db")
	} else {
		for i := 0; i < len(ElementsToRead); i = i + 1 {
			ElementsToReadArray = append(ElementsToReadArray, ElementsToRead[i][0])
		}
	}

	return ElementsToReadArray
}

// SaveRedisDB is a fuction with the scope to save
// the db data permanently
// The SAVE command performs a synchronous save of the dataset
// producing a point in taime snapshot of all data inside the Redis
// instance, in the form of an RDB file

// The SAVE command should almost never be called in production
// einvironments where it will block all the other clients.

func SaveRedisDB(r radix.Client) {

	r.Do(radix.Cmd(nil, "SAVE"))

}

// InitREDISQLDB function is used to initialize the DB, create the table
// if it doesn't exists. Insert data for the firt time if the table didn't exists
// otherwise, check if there are new files and eventually insert them in DB

/*
 * 	@params		r 				redisql client
 * 	@params		databaseName 		name of the db
 *	@params		tableName 			name of the table
 *	@params		filePathColumn 		name of the column where the file path values are saved
 *	@params		column1 			name of the first column - The first column is also the primry key
 *	@params		column2				name of the second column
 *	@params		column3				name of the third column
 *	@params		column4				name of the forth column
 * 	@params		directoryPath 		path of the directory where to read filenames from
 *
 *	This function returns true if the table exists, otherwise false
 *
 */

func InitREDISQLDB(r radix.Client, databaseName string, tableName string, column1 string, column2 string, column3 string, column4 string, directoryPath string) {

	log.Println("Create DB")
	CreateNewDB(r, databaseName)
	log.Println("Check if table already exists")
	table := TableExists(r, databaseName, tableName)
	if table == false {
		log.Println("Table does not exist. Creating table:", tableName)
		CreateDBTable(r, databaseName, tableName, column1, column2, column3, column4)
		log.Println("Getting file paths from the directory: ", directoryPath)
		DirectoryFilesPath := GetDirFilePaths(directoryPath)
		log.Println("Insert all file paths in the DB.")
		log.Println("NOTE: \t all other table values are set to False")
		InitInsertPathsInDB(r, databaseName, tableName, DirectoryFilesPath, "False", "False", "False")
		SaveRedisDB(r)
	} else {
		log.Println("Table exists")
		log.Println("Get file paths already saved in DB")

		FilesSavedInDB := GetPathsFromDB(r, databaseName, tableName, column1)

		log.Println("Get file paths from the directory")
		FilesInDirectory := GetDirFilePaths(directoryPath)

		log.Println("Check if there are new files")
		newFiles := GetPathsOfNewFiles(FilesInDirectory, FilesSavedInDB)

		if len(newFiles) == 0 {
			log.Println("All the files are currently saved in DB. There are no new files")
		} else {
			log.Println("Inserting new files")
			InitInsertPathsInDB(r, databaseName, tableName, newFiles, "False", "False", "False")
			SaveRedisDB(r)
		}
	}

	log.Println("Initializatione done!")
}
