package utils

import "os"

func openFile(fileName string) (*os.File, error) {
	fd, err := os.OpenFile(os.Getenv("DBPATH")+"db.bin", os.O_CREATE|os.O_RDWR|os.O_SYNC, 0755)
	if err != nil {
		return nil, err
	}
	return fd, err
}

func readFile(fd *os.File, len uint32, offset int64) ([]byte, error) {
	buf := make([]byte, len)
	_, err := fd.ReadAt(buf[:], offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func writeFile(fd *os.File, buf []byte, offset int64) error {
	_, err := fd.WriteAt(buf[:], offset)
	return err
}
