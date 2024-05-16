package ibd2schema

/* https://github.com/mysql/mysql-server/blob/trunk/sql/dd/types/column.h#L52 */
type ColumnType int

const (
	CT_DECIMAL ColumnType = iota + 1
	CT_TINY
	CT_SHORT
	CT_LONG
	CT_FLOAT
	CT_DOUBLE
	CT_TYPE_NULL
	CT_TIMESTAMP
	CT_LONGLONG
	CT_INT24
	CT_DATE
	CT_TIME
	CT_DATETIME
	CT_YEAR
	CT_NEWDATE
	CT_VARCHAR
	CT_BIT
	CT_TIMESTAMP2
	CT_DATETIME2
	CT_TIME2
	CT_NEWDECIMAL
	CT_ENUM
	CT_SET
	CT_TINY_BLOB
	CT_MEDIUM_BLOB
	CT_LONG_BLOB
	CT_BLOB
	CT_VAR_STRING
	CT_STRING
	CT_GEOMETRY
	CT_JSON
)

type HiddenType int64

const (
	/// The column is visible (a normal column)
	HT_VISIBLE HiddenType = iota + 1
	/// The column is completely invisible to the server
	HT_HIDDEN_SE
	/// The column is visible to the server but hidden from the user.
	/// This is used for i.e. implementing functional indexes.
	HT_HIDDEN_SQL
	/// User table column marked as INVISIBLE by using the column visibility
	/// attribute. Column is hidden from the user unless it is explicitly
	/// referenced in the statement. Column is visible to the server.
	HT_HIDDEN_USER
)

/* https://github.com/mysql/mysql-server/blob/trunk/sql/dd/types/index.h#L57 */
type IndexType int64

const (
	IT_PRIMARY IndexType = iota + 1
	IT_UNIQUE
	IT_MULTIPLE
	IT_FULLTEXT
	IT_SPATIAL
)

func (it IndexType) String() string {
	switch it {
	case IT_PRIMARY: return "primary"
	case IT_UNIQUE: return "unique"
	case IT_MULTIPLE: return "multiple"
	case IT_FULLTEXT: return "fulltext"
	case IT_SPATIAL: return "spatial"
	}
	return "unknown index type"
}

type IndexAlgorithm int64 // similar to ha_key_alg
const (
	IA_SE_SPECIFIC IndexAlgorithm = iota + 1
	IA_BTREE
	IA_RTREE
	IA_HASH
	IA_FULLTEXT
)

/* https://github.com/mysql/mysql-server/blob/trunk/sql/dd/types/foreign_key.h */
type FKRule int64
const (
	FK_RULE_NO_ACTION FKRule = iota + 1
	FK_RULE_RESTRICT
	FK_RULE_CASCADE
	FK_RULE_SET_NULL
	FK_RULE_SET_DEFAULT
)

type FKMatchOption int64 
const (
    FK_OPTION_NONE FKMatchOption = iota + 1
    FK_OPTION_PARTIAL
    FK_OPTION_FULL
)
