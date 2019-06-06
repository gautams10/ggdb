package utils

import (
	"errors"
	"strings"
)

//CommandType specifies the broad type of the command as an enum
type CommandType int

const (
	//Create to Create Table
	Create CommandType = iota
	//Insert a row in the Table
	Insert
	//Drop a table from the database
	Drop
	//Delete a row from the table
	Delete
	//Select query on the SQL table
	Select
	//Update the SQL table
	Update
	//Quit from the Database
	Quit
	//Describe the table definition
	Describe
	//Show lists all table names in the DB
	Show
	//Unrecognized Command
	Unrecognized
)

//Command is used to describe the command type and the store the statement
type Command struct {
	CmdType  CommandType
	CmdSlice []string
}

//ParseType is used to parse the type of command from the string and initialize the commandtype
func ParseType(str string) (Command, error) {
	str = strings.Replace(str, "\n", "", -1)
	strSlice := strings.Split(str, " ")
	var command Command
	switch strSlice[0] {
	case "exit":
		command = Command{CmdType: Quit, CmdSlice: strSlice}
	case "select":
		command = Command{CmdType: Select, CmdSlice: strSlice}
	case "delete":
		command = Command{CmdType: Delete, CmdSlice: strSlice}
	case "create":
		command = Command{CmdType: Create, CmdSlice: strSlice}
	case "insert":
		command = Command{CmdType: Insert, CmdSlice: strSlice}
	case "update":
		command = Command{CmdType: Update, CmdSlice: strSlice}
	case "drop":
		command = Command{CmdType: Drop, CmdSlice: strSlice}
	case "show":
		command = Command{CmdType: Show, CmdSlice: strSlice}
	case "describe":
		command = Command{CmdType: Describe, CmdSlice: strSlice}
	default:
		command = Command{CmdType: Unrecognized, CmdSlice: strSlice}
	}
	if command.CmdType == Unrecognized {
		return command, errors.New("Unrecognized Command")
	}
	return command, nil
}
