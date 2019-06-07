package main

import (
	"bufio"
	"db/utils"
	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

func prompt() {
	fmt.Printf("gg.db>")
}

func httpServe(db *utils.Db) {
	r := mux.NewRouter()
	r.Handle("/show", utils.HTTPShow(db))
	r.Handle("/describe/{tableName}", utils.HTTPDescribe(db))
	//r.Handle("/select/{tableName}", utils.HTTPSelect(db))
	//r.Handle("/create/", utils.HTTPCreate(db))
	http.ListenAndServe(":8080", r)
}

func cliServe(db *utils.Db) {
	reader := bufio.NewReader(os.Stdin)
	for {
		prompt()
		str, _ := reader.ReadString('\n')
		command, err := utils.ParseType(str)
		if err != nil {
			checkError(err)
			continue
		}
		switch command.CmdType {
		case utils.Create:
			checkError(utils.ExecCreate(db, command))
		case utils.Select:
			checkError(utils.ExecSelect(db, command))
		case utils.Quit:
			checkError(utils.ExecExit(db, command))
		case utils.Insert:
			checkError(utils.ExecInsert(db, command))
		case utils.Show:
			checkError(utils.ExecShow(db, command))
		case utils.Describe:
			checkError(utils.ExecDescribe(db, command))
		default:
			fmt.Printf("Unrecognized Command\n")
		}
	}
}
func main() {
	db, err := utils.Open()
	if err != nil {
		checkError(err)
		os.Exit(1)
	}
	defer db.Close()
	fmt.Println("Logged in to GG DB!")
	go httpServe(db)
	cliServe(db)
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
	}
}
