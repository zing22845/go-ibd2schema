package ibd2schema

import (
	"fmt"

	"github.com/tidwall/gjson"
)

// UTBitsInBytes converts a given number of bits to the minimum number of bytes needed to store them
func UTBitsInBytes(bits uint32) uint32 {
	return (bits + 7) / 8
}

func CheckMember(column gjson.Result, member string) error {
	if !column.Get(member).Exists() {
		return fmt.Errorf(`column member %s not found`, member)
	}
	return nil
}
