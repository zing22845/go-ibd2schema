package ibd2schema

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

var IndexMembers = []string{
	`name`,
	`type`,
	`hidden`,
	`elements`,
	`options`,
	`algorithm`,
	`is_algorithm_explicit`,
	`comment`,
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
	Name                string
	Type                IndexType
	Hidden              bool
	GJson               gjson.Result
	DDL                 string
	Algorithm           IndexAlgorithm
	IsAlgorithmExplicit bool
	Comment             string
}

func NewIndex(i gjson.Result) *Index {
	return &Index{
		Name:                i.Get("name").String(),
		Type:                IndexType(i.Get("type").Int()),
		Hidden:              i.Get("hidden").Bool(),
		GJson:               i,
		Algorithm:           IndexAlgorithm(i.Get("algorithm").Int()),
		IsAlgorithmExplicit: i.Get("is_algorithm_explicit").Bool(),
		Comment:             i.Get("comment").String(),
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
	case IT_FULLTEXT:
		i.DDL += fmt.Sprintf("  FULLTEXT KEY `%s` (", i.Name)
	case IT_SPATIAL:
		i.DDL += fmt.Sprintf("  SPATIAL KEY `%s` (", i.Name)
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
		i.DDL += fmt.Sprintf("(%s)", column.GenerationExpression)
	} else if i.Type == IT_FULLTEXT || i.Type == IT_SPATIAL {
		i.DDL += fmt.Sprintf("`%s`,", column.Name)
	} else {
		i.DDL += fmt.Sprintf("`%s`", column.Name)
		/* check prefix index */
		if column.SupportPrefixIndex() {
			if element.Length != int64(column.Size) {
				i.DDL += fmt.Sprintf("(%d)", element.Length/int64(column.Collation.Maxlen))
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
	i.DDL = strings.TrimSuffix(i.DDL, ",")
	i.DDL += ")"
	return nil
}

func (i *Index) parseAlgorithm() (err error) {
	if !i.IsAlgorithmExplicit {
		return nil
	}
	switch i.Algorithm {
	case IA_BTREE:
		i.DDL += " USING BTREE"
	case IA_HASH:
		i.DDL += " USING HASH"
	default:
		return fmt.Errorf("unsupported index algorithm %s", i.Algorithm.String())
	}
	return nil
}

func (i *Index) parseOptions() (err error) {
	options := i.GJson.Get("options")
	optionsList := strings.Split(options.String(), ";")
	for _, option := range optionsList {
		opt := strings.SplitN(option, "=", 2)
		switch opt[0] {
		case "":
			continue
		case "flags":
			if opt[1] != "0" {
				return fmt.Errorf("unsupported options flags %s", opt[1])
			}
		case "parser_name":
			i.DDL += fmt.Sprintf(" /*!50100 WITH PARSER `%s` */ ", opt[1])
		default:
			return fmt.Errorf("unsupported option %s", opt[0])
		}
	}
	i.DDL += ",\n"
	return nil
}

func (i *Index) parseComment() (err error) {
	if i.Comment != "" {
		i.DDL += fmt.Sprintf(" COMMENT '%s'", i.Comment)
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
		err = index.parseAlgorithm()
		if err != nil {
			return "", err
		}
		err = index.parseOptions()
		if err != nil {
			return "", err
		}
		err = index.parseComment()
		if err != nil {
			return "", err
		}
		ddl += index.DDL
	}
	return ddl, nil
}
