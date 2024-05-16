package ibd2schema

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
)

var ColumnMembers = []string{
	`name`,
	`type`,
	`is_nullable`,
	`is_zerofill`,
	`is_unsigned`,
	`is_auto_increment`,
	`is_virtual`,
	`hidden`,
	`ordinal_position`,
	`char_length`,
	`numeric_precision`,
	`numeric_scale`,
	`numeric_scale_null`,
	`datetime_precision`,
	`datetime_precision_null`,
	`has_no_default`,
	`default_value_null`,
	`srs_id_null`,
	`srs_id`,
	`default_value`,
	`default_value_utf8_null`,
	`default_value_utf8`,
	`default_option`,
	`update_option`,
	`comment`,
	`generation_expression`,
	`generation_expression_utf8`,
	`options`,
	`se_private_data`,
	`engine_attribute`,
	`secondary_engine_attribute`,
	`column_key`,
	`column_type_utf8`,
	`elements`,
	`collation_id`,
	`is_explicit_collation`,
}

type Column struct {
	OrdinalPosition      int
	Name                 string
	Type                 ColumnType
	GenerationExpression string
	Hidden               HiddenType
	Size                 uint64
	Collation            *Collation
	GJson                gjson.Result
	DDL                  string
}

func NewColumn(c gjson.Result) (*Column, error) {
	collation, err := GetCollationByID(int(c.Get(`collation_id`).Int()))
	if err != nil {
		return nil, err
	}
	return &Column{
		OrdinalPosition:      int(c.Get(`ordinal_position`).Int()) - 1,
		Name:                 c.Get(`name`).String(),
		GenerationExpression: c.Get(`generation_expression`).String(),
		Hidden:               HiddenType(c.Get(`hidden`).Int()),
		Type:                 ColumnType(c.Get(`type`).Int()),
		Size:                 uint64(c.Get(`char_length`).Uint()),
		Collation:            collation,
		GJson:                c,
	}, nil
}

/*
* Check if a column is hidden by user
 */
func (c *Column) isHiddenUser() bool {
	return c.Hidden == HT_HIDDEN_USER
}

/*
* Check if a column is hidden
 */
func (c *Column) isHidden() bool {
	return c.Hidden != HT_VISIBLE
}

/*
	Check if charset definition should be skipped

@return True if the charset definition should be skipped
*/
func (c *Column) skipCharset() bool {
	return c.Type == CT_JSON ||
		c.Type == CT_BLOB ||
		c.Type == CT_TINY_BLOB ||
		c.Type == CT_MEDIUM_BLOB ||
		c.Type == CT_LONG_BLOB
}

/*
	Check if column type support index prefix

@return True if the type supports index prefix
*/
func (c *Column) SupportPrefixIndex() bool {
	return c.Type == CT_VARCHAR ||
		c.Type == CT_TINY_BLOB ||
		c.Type == CT_MEDIUM_BLOB ||
		c.Type == CT_LONG_BLOB ||
		c.Type == CT_BLOB ||
		c.Type == CT_VAR_STRING ||
		c.Type == CT_STRING
}

/*
* Check if option string is gipk
@return false in case gipk=0, true otherwise
*/
func (c *Column) isGipk() bool {
	options := c.GJson.Get(`options`).String()
	pos := strings.Index(options, "gipk=")
	if pos == -1 {
		return false
	}
	start := pos + 5
	end := strings.Index(options[start:], ";")
	if end == -1 {
		end = len(options)
	}
	valueStr := options[start:end]
	valueStr = strings.Trim(valueStr, " ")
	// convert valueStr to integer
	value, _ := strconv.Atoi(valueStr)
	return value != 0
}

func (c *Column) parseName() {
	c.DDL += fmt.Sprintf("  `%s`", c.Name)
}

func (c *Column) parseStringAttribute(attribute string) {
	c.DDL += fmt.Sprintf(" %s", c.GJson.Get(attribute).String())
}

func (c *Column) parseCharset() {
	if c.GJson.Get("is_explicit_collation").Bool() {
		if c.skipCharset() {
			return
		}
		c.DDL += fmt.Sprintf(
			" CHARACTER SET %s COLLATE %s",
			c.Collation.CharsetName, c.Collation.Name)
	}
}

func (c *Column) parseGenerationExpression() {
	if c.GenerationExpression != "" {
		c.DDL += fmt.Sprintf(
			" GENERATED ALWAYS AS (%s)",
			c.GenerationExpression)
		if c.GJson.Get("is_virtual").Bool() {
			c.DDL += " VIRTUAL"
		} else {
			c.DDL += " STORED"
		}
	}
}

func (c *Column) parseIsNullable() {
	if !c.GJson.Get("is_nullable").Bool() {
		c.DDL += " NOT NULL"
	}
}

func (c *Column) parseDefaultValueNull() {
	/* skip default if is generated */
	if c.GenerationExpression != "" {
		return
	}
	defaultValueUTF8Null := c.GJson.Get("default_value_utf8_null").Bool()
	defaultValueUTF8 := c.GJson.Get("default_value_utf8").String()
	if c.GJson.Get("default_value_null").Bool() && defaultValueUTF8Null {
		c.DDL += " DEFAULT NULL"
	} else if !defaultValueUTF8Null {
		defaultOption := c.GJson.Get("default_option").String()
		if defaultOption == "" {
			c.DDL += fmt.Sprintf(" DEFAULT '%s'", defaultValueUTF8)
		} else {
			c.DDL += fmt.Sprintf(" DEFAULT %s", defaultOption)
		}
	}
}

func (c *Column) parseIsAutoIncrement() {
	if c.GJson.Get("is_auto_increment").Bool() {
		c.DDL += " AUTO_INCREMENT"
	}
}

func (c *Column) parseIsGipk() {
	if c.isGipk() {
		c.DDL += " /*!80023 INVISIBLE */"
	}
}

func (c *Column) parseDDL() {
	// parse ddl from attribues
	c.parseName()
	c.parseStringAttribute("column_type_utf8")
	c.parseCharset()
	c.parseGenerationExpression()
	c.parseIsNullable()
	c.parseDefaultValueNull()
	c.parseIsAutoIncrement()
	c.parseIsGipk()
}

type ColumnCache map[int]*Column

func (cc ColumnCache) AddColumn(c gjson.Result) (column *Column, err error) {
	column, err = NewColumn(c)
	if err != nil {
		return nil, err
	}
	cc[column.OrdinalPosition] = column
	return column, nil
}

func CheckColumnMembers(column gjson.Result) error {
	if !column.IsObject() {
		return fmt.Errorf("column is not an object")
	}
	for _, member := range ColumnMembers {
		if err := CheckMember(column, member); err != nil {
			return err
		}
	}
	return nil
}

func ParseColumns(ddObject gjson.Result) (
	ddl string, columnCache ColumnCache, err error) {
	columns := ddObject.Get(`columns`)
	if !columns.Exists() {
		return "", nil, fmt.Errorf(`table columns not found`)
	}
	columnCache = make(ColumnCache)
	for _, c := range columns.Array() {
		err = CheckColumnMembers(c)
		if err != nil {
			return "", nil, err
		}
		column, err := columnCache.AddColumn(c)
		if err != nil {
			return "", nil, err
		}
		if !column.isHiddenUser() && column.isHidden() {
			continue
		}
		// parse ddl
		column.parseDDL()
		ddl += fmt.Sprintf("%s,\n", column.DDL)
	}
	return ddl, columnCache, nil
}
