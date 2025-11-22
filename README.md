# go-ibd2schema

Parse MySQL 8.0 InnoDB .ibd files to SDI and convert SDI to table schema

## Overview

go-ibd2schema is a tool inspired by ibd2sdi and [sdi2ddl](https://github.com/altmannmarcelo/sdi2ddl), but with enhanced functionality and performance. It allows you to extract table schemas directly from MySQL 8.0 InnoDB .ibd files, offering a streamlined process for database schema analysis and migration.

## Why we wrote this

We developed go-ibd2schema to address a specific need in managing large MySQL databases with numerous tables. Our primary goals were:

1. To ensure consistency in table schemas when working with Xtrabackup results.
2. To extract schema information from physical files (specifically .ibd files for MySQL 8.x) while streaming to storage.

While existing tools like ibd2sdi can extract SDI from .ibd files, they are limited to working with files on disk. go-ibd2schema extends this functionality by:

- Allowing SDI extraction directly from memory
- Combining the functionality of ibd2sdi and sdi2ddl into a single step
- Converting the extracted SDI to DDL in one seamless process

This approach significantly improves efficiency when dealing with large-scale database operations, particularly in streaming or cloud environments where direct file access might be limited.

## Features

- Read .ibd files from both file system and data streams
- Direct parsing of .ibd files to schema without intermediate steps
- Support for fulltext index parsing
- Support for spatial index parsing
- Parsing of key parser configurations (e.g., `/*150100 WITH PARSER `ngram` */`)
- Parsing of `update_option` in table definitions (e.g., `ON UPDATE CURRENT_TIMESTAMP`)
- Improved performance compared to ibd2sdi + sdi2ddl combination

## Comparison with ibd2sdi + sdi2ddl

| Feature                   | go-ibd2schema | ibd2sdi + sdi2ddl            |
|---------------------------|---------------|------------------------------|
| Read .ibd from file       | ✅            | ✅                           |
| Read .ibd from data stream| ✅            | ❌                           |
| Direct schema parsing     | ✅            | ❌ (two-step process)        |
| Fulltext index support    | ✅            | ✅ (ibd2sdi) / ❌ (sdi2ddl)  |
| Spatial index support     | ✅            | ✅ (ibd2sdi) / ❌ (sdi2ddl)  |
| Key parser support        | ✅            | ✅ (ibd2sdi) / ❌ (sdi2ddl)  |
| update_option support     | ✅            | ✅ (ibd2sdi) / ❌ (sdi2ddl)  |
| Performance (file parsing)| 1.2~1.4x      | 1x                          |

## Usage

Just give the parser filepath and file reader of the .ibd file of MySQL 8.0 InnoDB tablespace.

```go
func main() {
 filePath := os.Args[1]
 file, err := os.Open(filePath)
 if err != nil {
  panic(err)
 }

 ts, err := ibd2schema.NewTableSpace(file, os.Stdout)
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
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.

## Acknowledgements

This project was inspired by ibd2sdi and [sdi2ddl](https://github.com/altmannmarcelo/sdi2ddl). We thank the creators of these tools for their work in the MySQL ecosystem.
