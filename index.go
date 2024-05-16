package ibd2schema

import (
	"fmt"

	"github.com/tidwall/gjson"
)

var IndexMembers = []string{
	`name`,
	`type`,
	`hidden`,
	`elements`,
}

var IndexElementMembers = []string{
	`length`,
	`hidden`,
	`column_opx`,
}

type IndexElement struct {
	Length    int64
	Hidden    bool
	ColumnOpx int
}

func NewIndexElement(e gjson.Result) *IndexElement {
	return &IndexElement{
		Length:    e.Get("length").Int(),
		Hidden:    e.Get("hidden").Bool(),
		ColumnOpx: int(e.Get("column_opx").Int()),
	}
}

type Index struct {
	Name   string
	Type   IndexType
	Hidden bool
	GJson  gjson.Result
	DDL    string
}

func NewIndex(i gjson.Result) *Index {
	return &Index{
		Name:   i.Get("name").String(),
		Type:   IndexType(i.Get("type").Int()),
		Hidden: i.Get("hidden").Bool(),
		GJson:  i,
	}
}

func (i *Index) parseType() (err error) {
	switch i.Type {
	case IT_PRIMARY:
		i.DDL += "  PRIMARY KEY ("
	case IT_UNIQUE:
		i.DDL += fmt.Sprintf("  UNIQUE KEY `%s` (", i.Name)
	case IT_MULTIPLE:
		i.DDL += fmt.Sprintf("  KEY `%s` (", i.Name)
	default:
		return fmt.Errorf("unsuported index type %d", i.Type)
	}
	return nil
}

func (i *Index) parseElementDDL(element *IndexElement, columnCache ColumnCache) (err error) {
	column, ok := columnCache[element.ColumnOpx]
	if !ok {
		return fmt.Errorf("index column %d not found in the column map", element.ColumnOpx)
	}
	if i.Type == IT_MULTIPLE && column.Hidden == HT_HIDDEN_SQL {
		i.DDL += fmt.Sprintf("(%s),", column.GenerationExpression)
	} else {
		i.DDL += fmt.Sprintf("`%s`", column.Name)
		/* check prefix index */
		if column.SupportPrefixIndex() {
			if element.Length != int64(column.Size) {
				i.DDL += fmt.Sprintf("(%d)", element.Length)
			}
		}
		i.DDL += ","
	}
	return nil
}

func (i *Index) parseElements(columnCache ColumnCache) (err error) {
	indexes := i.GJson.Get("elements")
	for _, e := range indexes.Array() {
		err = CheckIndexElementMembers(e)
		if err != nil {
			return err
		}
		element := NewIndexElement(e)
		if element.Hidden {
			// skip hidden elements
			continue
		}
		err = i.parseElementDDL(element, columnCache)
		if err != nil {
			return err
		}
	}
	return nil
}

func CheckIndexMembers(index gjson.Result) error {
	if !index.IsObject() {
		return fmt.Errorf("index is not an object")
	}
	for _, member := range IndexMembers {
		if err := CheckMember(index, member); err != nil {
			return err
		}
	}
	return nil
}

func CheckIndexElementMembers(element gjson.Result) error {
	if !element.IsObject() {
		return fmt.Errorf("index element is not an object")
	}
	for _, member := range IndexElementMembers {
		if err := CheckMember(element, member); err != nil {
			return err
		}
	}
	return nil
}

/*
* Parse the indexes section of SDI JSON
@param[in]	    dd_object	    Data Dictionary JSON object
@param[in,out]	ddl     	    DDL string
@return False in case of errors
*/
func ParseIndexes(ddObject gjson.Result, columnCache ColumnCache) (
	ddl string, err error) {
	indexes := ddObject.Get(`indexes`)
	if !indexes.Exists() {
		return "", fmt.Errorf(`table indexes not found`)
	}
	for _, i := range indexes.Array() {
		err = CheckIndexMembers(i)
		if err != nil {
			return "", err
		}
		index := NewIndex(i)
		if index.Hidden {
			// skip hidden indexes
			continue
		}
		// parse attributes
		err = index.parseType()
		if err != nil {
			return "", err
		}
		err = index.parseElements(columnCache)
		if err != nil {
			return "", err
		}
		index.DDL = index.DDL[:len(index.DDL)-1]
		ddl += fmt.Sprintf("%s),\n", index.DDL)
	}
	return ddl, nil
}
