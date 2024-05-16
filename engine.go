package ibd2schema

import (
	"fmt"

	"github.com/tidwall/gjson"
)

/*
* Parse the engine section of SDI JSON
@param[in]	    dd_object	Data Dictionary JSON object
@param[in,out]	ddl     	DDL string
@return False in case of errors
*/
func ParseEngine(ddlObject gjson.Result) (ddl string, err error) {
	engine := ddlObject.Get("engine")
	if !engine.Exists() {
		return "", fmt.Errorf("engine not found")
	}
	ddl += " ENGINE="
	ddl += engine.String()
	return ddl, nil
}
