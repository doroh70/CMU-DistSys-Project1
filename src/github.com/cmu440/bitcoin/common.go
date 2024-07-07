package bitcoin

import (
	"log"
	"os"
)

var (
	INFO  *log.Logger
	WARN  *log.Logger
	ERROR *log.Logger
)

const (
	name = "clientLog.txt"
	flag = os.O_RDWR | os.O_CREATE
	perm = os.FileMode(0666)
)

func InitLoggers() (*os.File, error) {
	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	INFO = log.New(file, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	WARN = log.New(file, "WARN: ", log.Ldate|log.Ltime|log.Lshortfile)
	ERROR = log.New(file, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

	return file, nil
}
