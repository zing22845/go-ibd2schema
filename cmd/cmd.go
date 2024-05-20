package main

import (
	"fmt"
	"os"

	"github.com/tidwall/pretty"
	ibd2schema "github.com/zing22845/go-ibd2schema"
)

func main() {
	filePath := os.Args[1]
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	ts, err := ibd2schema.NewTableSpace(file)
	if err != nil {
		panic(err)
	}
	// dump ddl: only support file per table = On
	err = ts.DumpSchemas()
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(-1)
	}
	for db, table := range ts.TableSchemas {
		fmt.Printf("Database: %s\n", db)
		fmt.Printf("Table DDL: %s\n", table.DDL)
	}
	// dump sdi
	err = ts.DumpSDIs()
	if err != nil {
		fmt.Printf("%+v\n", err)
		os.Exit(-1)
	}
	fmt.Println(string(pretty.Pretty(ts.SDIResult)))
}
