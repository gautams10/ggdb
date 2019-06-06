package utils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strconv"
	"unsafe"
)

const maxStrSize int = 16
const sizeInt = 4
const maxAllocSize = 0xFFFFFFF

//Db stores information for a given Db instance
type Db struct {
	fileDescriptor *os.File
	meta           *dbMeta
	tablePtrs      map[string]*table
	tableMetaPtrs  map[string]*tableMeta
	needFlush      bool
}
type dbMeta struct {
	pageSize   uint32
	tableCount uint32
}
type table struct {
	fileDescriptor *os.File
	tableMetaPtr   *tableMeta
	needFlush      bool
	freelist       []rID
	pages          map[uint32]*page
}
type attribute struct {
	coloumnName [maxStrSize]byte
	coloumnType [maxStrSize]byte
}
type tableMeta struct {
	tableName      [maxStrSize]byte
	rowCount       uint32
	rowSize        uint32
	attributeCount uint32
	attributes     []*attribute
}

type page struct {
	pgID   uint32
	rowPtr map[uint32]*row
	//	space  uint32
	dirty bool
}
type row struct {
	slotID uint32
	data   *[]byte
}
type rID struct {
	pgID   uint32
	slotID uint32
}

func (db *Db) init() {
	buf := make([]byte, os.Getpagesize())
	db.meta = (*dbMeta)(ptr(buf[:]))
	db.meta.pageSize = uint32(os.Getpagesize())
	db.meta.tableCount = uint32(0)
	//write db meta data to the db file
	db.fileDescriptor.WriteAt(buf, 0)
}

//Open the database, setting up the db instance
func Open() (*Db, error) {
	var db = &Db{needFlush: false, tableMetaPtrs: make(map[string]*tableMeta), tablePtrs: make(map[string]*table)}
	var err error
	db.fileDescriptor, err = os.OpenFile(os.Getenv("DBPATH")+"dbInfo.bin", os.O_CREATE|os.O_RDWR|os.O_SYNC, 0755)
	if err != nil {
		return nil, err
	}
	if info, err := db.fileDescriptor.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		db.init()
	} else {
		buf := make([]byte, os.Getpagesize())
		db.fileDescriptor.ReadAt(buf[:], 0)

		//Initialize the meta data
		db.meta = (*dbMeta)(ptr(buf[:]))
	}

	err = db.tables()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func ptr(buf []byte) unsafe.Pointer {
	return unsafe.Pointer(&buf[0])
}

//Initializes Db tables Meta and table data and sets them to db members - tableMetaPtrs, tablePtrs
func (db *Db) tables() error {

	buf := make([]byte, db.meta.pageSize)
	for i := uint32(1); i <= uint32(db.meta.tableCount); i++ {
		db.fileDescriptor.ReadAt(buf[:], int64(i*db.meta.pageSize))
		tblMeta := tableMeta{}
		offset := 0
		copy(tblMeta.tableName[:], buf[0:maxStrSize])
		offset += maxStrSize
		tblMeta.rowCount = binary.LittleEndian.Uint32(buf[offset : offset+sizeInt])
		offset += sizeInt
		tblMeta.rowSize = binary.LittleEndian.Uint32(buf[offset : offset+sizeInt])
		offset += sizeInt
		tblMeta.attributeCount = binary.LittleEndian.Uint32(buf[offset : offset+sizeInt])
		offset += sizeInt
		//fmt.Printf("VBVBVBVBV%d\n", tblMeta.attributeCount)
		for j := uint32(0); j < tblMeta.attributeCount; j++ {
			var attr = &attribute{}
			if offset+int(unsafe.Sizeof(*attr)) > int(db.meta.pageSize) {
				return errors.New("Couldn't load tables for the database, meta data overflows from the pagesize for the table - " + string(tblMeta.tableName[:]))
			}
			//append attributes
			attrSize := int(unsafe.Sizeof(*attr))
			tempBuf := make([]byte, attrSize)
			copy(tempBuf, buf[offset:offset+attrSize])
			attr = (*attribute)(ptr(tempBuf))
			tblMeta.attributes = append(tblMeta.attributes, attr)
			offset += attrSize
		}
		tbl, err := openTable(&tblMeta)
		if err != nil {
			return err
		}
		tableName := string(bytes.Trim(tblMeta.tableName[:], "\x00"))
		db.tableMetaPtrs[tableName] = &tblMeta
		db.tablePtrs[tableName] = tbl
	}
	return nil
}

//CreateTable is used to create a table and initialize it to the db instance
func (db *Db) CreateTable(tableName string, attributes map[string]string) error {

	//Check if the table already exists
	if _, ok := db.tableMetaPtrs[tableName]; ok {
		return errors.New("Couldn't create table as it already exists")
	}

	//Check if table data to be entered is valid
	if len(tableName) > maxStrSize {
		return errors.New("Table Name is too long")
	}
	for key, value := range attributes {
		if len(key) > maxStrSize {
			return errors.New("Table attribute names are too long")
		}
		if value != "char" && value != "int" {
			return errors.New("Attribute Type doesn't match supported types")
		}
	}

	//Initialize the table
	return db.initTable(tableName, attributes)
}

//Initializes a fresh table and updates db params with the freshly created table info returns an error if unsuccessful at the same.
func (db *Db) initTable(tableName string, attributes map[string]string) error {

	//needFlush is set to true as the table needs to written as it's a fresh table
	var tbl = &table{needFlush: false, pages: make(map[uint32]*page)}
	var err error
	//Open the file and set file descriptor
	tbl.fileDescriptor, err = os.OpenFile(os.Getenv("DBPATH")+tableName+".bin", os.O_CREATE|os.O_RDWR|os.O_SYNC, 0755)
	if err != nil {
		return err
	}

	//Initialize table meta data and set it
	tblMetaData := &tableMeta{rowCount: 0, attributeCount: uint32(len(attributes)), attributes: nil}
	copy(tblMetaData.tableName[:], []byte(tableName))
	var rowSize uint32
	for key, value := range attributes {
		attr := attribute{}
		copy(attr.coloumnName[:], []byte(key))
		copy(attr.coloumnType[:], []byte(value))
		tblMetaData.attributes = append(tblMetaData.attributes, &attr)
		if value == "char" {
			rowSize += uint32(maxStrSize)
		} else if value == "int" {
			rowSize += sizeInt
		}
	}
	tblMetaData.rowSize = rowSize
	tbl.tableMetaPtr = tblMetaData

	//Update the new table to db instance
	db.tableMetaPtrs[tableName] = tbl.tableMetaPtr
	db.tablePtrs[tableName] = tbl
	db.meta.tableCount++
	db.needFlush = true
	return nil
}

func openTable(tblMetaData *tableMeta) (*table, error) {
	//needFlush is set to False as the table needs to written as it's an existing table
	var tbl = &table{needFlush: false, pages: make(map[uint32]*page), tableMetaPtr: tblMetaData}
	var err error
	//Open the file and set file descriptor
	tbl.fileDescriptor, err = os.OpenFile(os.Getenv("DBPATH")+string(bytes.Trim(tblMetaData.tableName[:], "\x00"))+".bin", os.O_RDWR|os.O_SYNC, 0600)
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func (db *Db) flushDb() {
	if !db.needFlush {
		return
	}
	pageSize := db.meta.pageSize
	ptr := (*[maxAllocSize]byte)(unsafe.Pointer(db.meta))
	buf := make([]byte, pageSize)
	//Write the first page which contains the Db Meta data
	buf = ptr[:db.meta.pageSize]
	db.fileDescriptor.WriteAt(buf, 0)

	//Write a page for each table
	var i uint32
	for _, tableMetaData := range db.tableMetaPtrs {
		offset := 0
		copy(buf, tableMetaData.tableName[:])
		offset += maxStrSize
		binary.LittleEndian.PutUint32(buf[offset:], tableMetaData.rowCount)
		offset += sizeInt
		binary.LittleEndian.PutUint32(buf[offset:], tableMetaData.rowSize)
		offset += sizeInt
		binary.LittleEndian.PutUint32(buf[offset:], tableMetaData.attributeCount)
		offset += sizeInt
		for j := uint32(0); j < tableMetaData.attributeCount; j++ {
			attributePtr := tableMetaData.attributes[j]
			attributeSize := int(unsafe.Sizeof(*attributePtr))
			ptr := (*[maxAllocSize]byte)(unsafe.Pointer(attributePtr))
			copy(buf[offset:], ptr[:attributeSize])
			offset += attributeSize
		}
		db.fileDescriptor.WriteAt(buf, int64((i+1)*pageSize))
	}
}
func flushTable(tbl *table, pageSize int64) {
	if !tbl.needFlush {
		return
	}
	rowSize := tbl.tableMetaPtr.rowSize
	buf := make([]byte, pageSize)
	for pgNum, page := range tbl.pages {
		offset := 0
		for _, r := range page.rowPtr {
			copy(buf[offset:], *r.data)
			offset += int(rowSize)
		}
		tbl.fileDescriptor.WriteAt(buf, pageSize*int64(pgNum))
	}
}

//ShowTables lists all table names
func (db *Db) ShowTables() []string {
	var out []string
	for _, metaPtr := range db.tableMetaPtrs {
		out = append(out, string(bytes.Trim(metaPtr.tableName[:], "\x00")))
	}
	return out
}

//DescribeTable Describes the structure of the table in the Db
func (db *Db) DescribeTable(tableName string) (map[string]string, error) {
	if tbl, ok := db.tableMetaPtrs[tableName]; ok {
		retAttr := make(map[string]string)
		for _, attr := range tbl.attributes {
			retAttr[string(bytes.Trim(attr.coloumnName[:], "\x00"))] = string(bytes.Trim(attr.coloumnType[:], "\x00"))
		}
		return retAttr, nil
	}
	return nil, errors.New("Table " + tableName + " doesn't Exist")
}

//Close the Db initialization, writing to the files for all the pending updates
func (db *Db) Close() {
	for _, tbl := range db.tablePtrs {
		flushTable(tbl, int64(db.meta.pageSize))
		tbl.fileDescriptor.Close()
	}
	db.flushDb()
	db.fileDescriptor.Close()
	return
}

func createRowBuf(tbl *table, values map[string]string) ([]byte, error) {
	buf := make([]byte, tbl.tableMetaPtr.rowSize)
	offset := 0
	for _, attr := range tbl.tableMetaPtr.attributes {
		attrType := string(bytes.Trim(attr.coloumnType[:], "\x00"))
		attrName := string(bytes.Trim(attr.coloumnName[:], "\x00"))
		if attrType == "char" {
			if len(values[attrName]) > maxStrSize {
				return nil, errors.New("String " + values[attrName] + " is too long")
			}
			copy(buf[offset:], []byte(values[attrName]))
			offset += maxStrSize
		} else if attrType == "int" {
			insertValue, err := strconv.ParseUint(values[attrName], 10, 32)
			if err != nil {
				return nil, errors.New("Couldn't store " + values[attrName] + " in Int")
			}
			binary.LittleEndian.PutUint32(buf[offset:], uint32(insertValue))
			offset += sizeInt
		}
	}
	return buf, nil
}

// InsertRow is used to insert values into a given tablename
func (db *Db) InsertRow(tableName string, values map[string]string) error {
	if _, ok := db.tablePtrs[tableName]; !ok {
		return errors.New("Table " + tableName + " Doesn't Exist")
	}
	tbl := db.tablePtrs[tableName]
	tblMeta := tbl.tableMetaPtr
	buf, err := createRowBuf(tbl, values)
	if err != nil {
		return err
	}
	tbl.needFlush = true
	rID := getRID(tblMeta.rowCount, tblMeta.rowSize, db.meta.pageSize)
	pg := getPage(tbl, rID.pgID, db.meta.pageSize)
	pg.rowPtr[rID.slotID] = &row{slotID: rID.slotID, data: &buf}
	tblMeta.rowCount++
	pg.dirty = true
	return nil
}

func (pg *page) readPageFromBuf(buf []byte, rowSize uint32) {
	tempBuf := make([]byte, rowSize)
	for offset, i := 0, 0; offset+int(rowSize) <= len(buf); offset += int(rowSize) {
		copy(tempBuf[:], buf[offset:offset+int(rowSize)])
		pg.rowPtr[uint32(i)] = &row{slotID: uint32(i), data: &tempBuf}
		i++
	}
}

func convRowToMap(tbl *table, r *row) map[string]string {
	m := make(map[string]string)
	tblMeta := tbl.tableMetaPtr
	buf := *r.data
	offset := 0
	for _, attr := range tblMeta.attributes {
		var value string
		cType := string(bytes.Trim(attr.coloumnType[:], "\x00"))
		cName := string(bytes.Trim(attr.coloumnName[:], "\x00"))
		if cType == "char" {
			value = string(buf[offset : offset+maxStrSize])
			offset += maxStrSize
		}
		if cType == "int" {
			tempValue := binary.LittleEndian.Uint32(buf[offset : offset+sizeInt])
			offset += maxStrSize
			value = strconv.Itoa(int(tempValue))
		}
		m[cName] = value
	}
	return m
}

//SelectRow used to print Rows of Db
func (db *Db) SelectRow(tableName string) ([]map[string]string, error) {
	if tbl, ok := db.tablePtrs[tableName]; ok {
		tblMeta := tbl.tableMetaPtr
		retBuf := make([]map[string]string, tblMeta.rowCount)
		for i := uint32(0); i < tblMeta.rowCount; i++ {
			rid := getRID(i, tblMeta.rowSize, db.meta.pageSize)
			pg := getPage(tbl, rid.pgID, db.meta.pageSize)
			r := pg.rowPtr[rid.slotID]
			attrVal := convRowToMap(tbl, r)
			retBuf = append(retBuf, attrVal)
		}
		return retBuf, nil
	}
	return nil, errors.New("Table " + tableName + " Doesn't Exist")
}

func getRID(rowNum uint32, rowSize uint32, pageSize uint32) rID {
	rowsPerPage := pageSize / rowSize
	pgID := rowNum / rowsPerPage
	slotID := rowNum % rowsPerPage
	return rID{pgID: pgID, slotID: slotID}
}
func getPage(tbl *table, pgID uint32, pageSize uint32) *page {
	if pg, ok := tbl.pages[pgID]; ok {
		return pg
	}
	buf := make([]byte, pageSize)
	pg := &page{pgID: pgID, dirty: false, rowPtr: make(map[uint32]*row)}
	tbl.fileDescriptor.ReadAt(buf, int64(pgID*pageSize))
	pg.readPageFromBuf(buf, tbl.tableMetaPtr.rowSize)
	tbl.pages[pgID] = pg
	return pg
}
