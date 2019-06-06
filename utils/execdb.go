package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/mux"
)

//ExecCreate is used to create a table in the database
func ExecCreate(db *Db, command Command) error {
	tableName := command.CmdSlice[1]
	attributes := make(map[string]string)
	if len(command.CmdSlice)%2 != 0 || len(command.CmdSlice) <= 2 {
		return errors.New("Invalid Create Command, create <tablename> <attribute> <attributeType>")
	}

	for i := 2; i < len(command.CmdSlice); i += 2 {
		attributes[command.CmdSlice[i]] = command.CmdSlice[i+1]
	}
	return db.CreateTable(tableName, attributes)
}

//ExecShow is used to list all tables in the database
func ExecShow(db *Db, command Command) error {
	out := db.ShowTables()
	for _, tableName := range out {
		fmt.Println(tableName)
	}
	return nil
}

//ExecDescribe is used to list all tables in the database
func ExecDescribe(db *Db, command Command) error {
	if len(command.CmdSlice) != 2 {
		return errors.New("Invalid Describe Command, describe <tablename>")
	}
	tableName := command.CmdSlice[1]
	val, err := db.DescribeTable(tableName)
	if err != nil {
		return err
	}
	for key, value := range val {
		fmt.Printf("%s %s\n", key, value)
	}
	return nil
}

//ExecSelect is used to list all rows in the table
func ExecSelect(db *Db, command Command) error {
	if len(command.CmdSlice) != 2 {
		return errors.New("Invalid Select Command, select <tablename>")
	}
	tableName := command.CmdSlice[1]
	val, err := db.SelectRow(tableName)
	if err != nil {
		return err
	}
	for _, m := range val {
		var str string
		for _, value := range m {
			str = str + value + " "
		}
		fmt.Println(str)
	}
	return nil
}

//ExecExit handles the execution of non db commands such as exit etc.
func ExecExit(db *Db, command Command) error {
	fmt.Printf("Exiting GG Db!\n")
	db.Close()
	os.Exit(0)
	return nil
}

//ExecInsert handles inserting of rows into the database
func ExecInsert(db *Db, command Command) error {
	db.Close()
	if len(command.CmdSlice) < 2 {
		return errors.New("Invalid Insert command - insert <tablename> [<attributeName> <attrributeValues>]")
	}
	cmd := command.CmdSlice
	tblName := cmd[1]
	if _, ok := db.tableMetaPtrs[tblName]; !ok {
		return errors.New("Table Doesn't Exist")
	}
	if len(cmd)/2-1 != len(db.tableMetaPtrs[tblName].attributes) {
		return errors.New("All parameters aren't initialized")
	}
	attr := make(map[string]string)
	for i := 2; i < len(cmd); i += 2 {
		attr[cmd[i]] = cmd[i+1]
	}
	return db.InsertRow(tblName, attr)
}

//HTTPShow prints the show tabe output
func HTTPShow(db *Db) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		data := db.ShowTables()
		json.NewEncoder(w).Encode(data)
	})
}

//HTTPDescribe describes the structure of the database
func HTTPDescribe(db *Db) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		tableName := vars["tableName"]
		val, err := db.DescribeTable(tableName)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(val)
	})
}

//HTTPSelect displays all rows in the database
func HTTPSelect(db *Db) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		tableName := vars["tableName"]
		val, err := db.DescribeTable(tableName)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(val)
	})
}

//HTTPSelect displays all rows in the database
func HTTPSelect(db *Db) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		tableName := vars["tableName"]
		val, err := db.SelectRow(tableName)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(val)
	})
}
