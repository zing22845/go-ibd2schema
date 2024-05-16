package ibd2schema

import (
	"fmt"

	"github.com/tidwall/gjson"
)

type SDI struct {
	Type                uint64
	ID                  uint64
	OriginData          []byte
	OriginDataLen       uint64
	UncompressedData    []byte
	UncompressedDataLen uint64
	DDL                 string
}

func (sdi *SDI) DumpJson() (result []byte) {
	result = []byte(fmt.Sprintf(`{"type":%d,"id":%d,"object":`, sdi.Type, sdi.ID))
	result = append(result, sdi.UncompressedData...)
	result = append(result, []byte(`}`)...)
	return result
}

func (sdi *SDI) DumpDDL() (err error) {
	object := gjson.ParseBytes(sdi.UncompressedData)
	ddObjectType := object.Get(`dd_object_type`)
	if ddObjectType.String() != `Table` {
		return nil
	}
	ddObject := object.Get(`dd_object`)
	// table name
	name := ddObject.Get(`name`)
	if !name.Exists() {
		return fmt.Errorf(`table name not found`)
	}
	sdi.DDL = fmt.Sprintf("CREATE TABLE `%s` (\n", name.String())
	// parse table collation
	tableCollationDDL, err := ParseCollation(ddObject)
	if err != nil {
		return err
	}
	// table schema
	tableSchema := ddObject.Get(`schema_ref`)
	if !tableSchema.Exists() {
		return fmt.Errorf(`table schema not found`)
	}
	// table columns
	columnDDL, columnCache, err := ParseColumns(ddObject)
	if err != nil {
		return err
	}
	sdi.DDL += columnDDL
	// table indexes
	indexDDL, err := ParseIndexes(ddObject, columnCache)
	if err != nil {
		return err
	}
	sdi.DDL += indexDDL
	// foreign keys
	fkDDL, err := ParseForeignKeys(ddObject, columnCache, tableSchema.String())
	if err != nil {
		return err
	}
	sdi.DDL += fkDDL
	// enclose column and index
	sdi.DDL = sdi.DDL[:len(sdi.DDL)-2]
	sdi.DDL += "\n)"
	// engine
	engineDDL, err := ParseEngine(ddObject)
	if err != nil {
		return err
	}
	sdi.DDL += engineDDL
	// table collation
	sdi.DDL += tableCollationDDL
	return nil
}
