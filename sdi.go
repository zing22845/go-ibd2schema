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
	DatabaseName        string
	TableSchema         *TableSchema
}

func (sdi *SDI) DumpJson() (result []byte) {
	result = []byte(fmt.Sprintf(`{"type":%d,"id":%d,"object":`, sdi.Type, sdi.ID))
	result = append(result, sdi.UncompressedData...)
	result = append(result, []byte(`}`)...)
	return result
}

func (sdi *SDI) DumpTableSchema() (err error) {
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
	// hidden
	sdi.TableSchema = &TableSchema{
		Name:   name.String(),
		Hidden: HiddenType(ddObject.Get(`hidden`).Int()),
	}
	if sdi.TableSchema.Hidden != HT_VISIBLE {
		return nil
	}
	sdi.TableSchema.DDL = fmt.Sprintf("CREATE TABLE `%s` (\n", sdi.TableSchema.Name)
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
	sdi.DatabaseName = tableSchema.String()
	// table columns
	columnDDL, columnCache, err := ParseColumns(ddObject)
	if err != nil {
		return err
	}
	sdi.TableSchema.DDL += columnDDL
	// table indexes
	indexDDL, err := ParseIndexes(ddObject, columnCache)
	if err != nil {
		return err
	}
	sdi.TableSchema.DDL += indexDDL
	// foreign keys
	fkDDL, err := ParseForeignKeys(ddObject, columnCache, tableSchema.String())
	if err != nil {
		return err
	}
	sdi.TableSchema.DDL += fkDDL
	// enclose column and index
	sdi.TableSchema.DDL = sdi.TableSchema.DDL[:len(sdi.TableSchema.DDL)-2]
	sdi.TableSchema.DDL += "\n)"
	// engine
	engineDDL, err := ParseEngine(ddObject)
	if err != nil {
		return err
	}
	sdi.TableSchema.DDL += engineDDL
	// table collation
	sdi.TableSchema.DDL += tableCollationDDL
	// table comment
	tableComment := ddObject.Get(`comment`)
	if !tableComment.Exists() {
		return fmt.Errorf(`table comment not found`)
	}
	if tableComment.String() != "" {
		sdi.TableSchema.DDL += fmt.Sprintf(" COMMENT = '%s'", tableComment.String())
	}
	return nil
}
