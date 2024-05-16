package ibd2schema

import (
	"fmt"

	"github.com/tidwall/gjson"
)

var ForeignKeyMembers = []string{
	`name`,
	`elements`,
	`update_rule`,
	`delete_rule`,
	`referenced_table_schema_name`,
	`referenced_table_name`,
}

var ForeignKeyElementMembers = []string{
	`referenced_column_name`,
	`column_opx`,
}

type ForeignKeyElement struct {
	ReferencedColumnName string
	ColumnOpx            int
}

func NewForeignKeyElement(element gjson.Result) *ForeignKeyElement {
	return &ForeignKeyElement{
		ReferencedColumnName: element.Get(`referenced_column_name`).String(),
		ColumnOpx:            int(element.Get(`column_opx`).Int()),
	}
}

type ForeignKey struct {
	Name                      string
	UpdateRule                FKRule
	DeleteRule                FKRule
	ReferenceNames            []string
	ReferencedTableSchemaName string
	ReferencedTableName       string
	Gjson                     gjson.Result
	DDL                       string
}

func NewForeignKey(fk gjson.Result) *ForeignKey {
	return &ForeignKey{
		Name:                      fk.Get(`name`).String(),
		UpdateRule:                FKRule(fk.Get(`update_rule`).Int()),
		DeleteRule:                FKRule(fk.Get(`delete_rule`).Int()),
		ReferenceNames:            make([]string, 0),
		ReferencedTableSchemaName: fk.Get(`referenced_table_schema_name`).String(),
		ReferencedTableName:       fk.Get(`referenced_table_name`).String(),
		Gjson:                     fk,
	}
}

func (fk *ForeignKey) parseName() {
	fk.DDL += fmt.Sprintf("  CONSTRAINT `%s` FOREIGN KEY (", fk.Name)
}

func (fk *ForeignKey) parseElementDDL(element *ForeignKeyElement, columnCache ColumnCache) (err error) {
	column, ok := columnCache[element.ColumnOpx]
	if !ok {
		return fmt.Errorf("index column %d not found in the column map", element.ColumnOpx)
	}
	fk.ReferenceNames = append(fk.ReferenceNames, element.ReferencedColumnName)
	fk.DDL += fmt.Sprintf("`%s`, ", column.Name)
	return nil
}

func (fk *ForeignKey) parseElements(columnCache ColumnCache) (err error) {
	elements := fk.Gjson.Get(`elements`)
	for _, e := range elements.Array() {
		err = CheckForeignKeyElementMembers(e)
		if err != nil {
			return err
		}
		fkElement := NewForeignKeyElement(e)
		err = fk.parseElementDDL(fkElement, columnCache)
		if err != nil {
			return err
		}
	}
	fk.DDL = fk.DDL[:len(fk.DDL)-2] + ") REFERENCES "
	return nil
}

func (fk *ForeignKey) parseReference(tableSchema string) {
	if fk.ReferencedTableSchemaName != tableSchema {
		fk.DDL += fmt.Sprintf("`%s`.`%s` (", fk.ReferencedTableSchemaName, fk.ReferencedTableName)
	} else {
		fk.DDL += fmt.Sprintf("`%s` (", fk.ReferencedTableName)
	}
	for _, rn := range fk.ReferenceNames {
		fk.DDL += fmt.Sprintf("`%s`, ", rn)
	}
	fk.DDL = fk.DDL[:len(fk.DDL)-2] + ")"
}

func (fk *ForeignKey) parseAction() {
	// ON DELETE
	switch fk.DeleteRule {
	case FK_RULE_RESTRICT:
		fk.DDL += " ON DELETE RESTRICT"
	case FK_RULE_CASCADE:
		fk.DDL += " ON DELETE CASCADE"
	case FK_RULE_SET_NULL:
		fk.DDL += " ON DELETE SET NULL"
	case FK_RULE_SET_DEFAULT:
		fk.DDL += " ON DELETE SET DEFAULT"
	case FK_RULE_NO_ACTION:
	default:
	}
	// ON UPDATE
	switch fk.UpdateRule {
	case FK_RULE_RESTRICT:
		fk.DDL += " ON UPDATE RESTRICT"
	case FK_RULE_CASCADE:
		fk.DDL += " ON UPDATE CASCADE"
	case FK_RULE_SET_NULL:
		fk.DDL += " ON UPDATE SET NULL"
	case FK_RULE_SET_DEFAULT:
		fk.DDL += " ON UPDATE SET DEFAULT"
	case FK_RULE_NO_ACTION:
	default:
	}
}

func CheckForeignKeyMembers(fk gjson.Result) error {
	if !fk.IsObject() {
		return fmt.Errorf("foreign key is not an object")
	}
	for _, member := range ForeignKeyMembers {
		if err := CheckMember(fk, member); err != nil {
			return err
		}
	}
	return nil
}

func CheckForeignKeyElementMembers(element gjson.Result) error {
	if !element.IsObject() {
		return fmt.Errorf("foreign key element is not an object")
	}
	for _, member := range ForeignKeyElementMembers {
		if err := CheckMember(element, member); err != nil {
			return err
		}
	}
	return nil
}

func ParseForeignKeys(ddObject gjson.Result, columnCache ColumnCache, tableSchema string) (
	ddl string, err error) {
	foreignKeys := ddObject.Get(`foreign_keys`)
	if !foreignKeys.Exists() {
		return "", fmt.Errorf(`table foreign keys not found`)
	}
	for _, fk := range foreignKeys.Array() {
		err = CheckForeignKeyMembers(fk)
		if err != nil {
			return "", err
		}
		foreignKey := NewForeignKey(fk)
		foreignKey.parseName()
		err = foreignKey.parseElements(columnCache)
		if err != nil {
			return "", err
		}
		foreignKey.parseReference(tableSchema)
		foreignKey.parseAction()
		ddl += fmt.Sprintf(foreignKey.DDL + ",\n")
	}
	return ddl, nil
}
