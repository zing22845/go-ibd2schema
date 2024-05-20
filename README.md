# go-ibd2schema

Parse ibd file of MySQL 8.0 to SDI and convert SDI to table schema(inspired by ibd2sdi and sdi2ddl)

## Why we write this

We have a MySQL database has lots of tables and we need the consistency table schema of the Xtrabackup result.
When streaming to storage, we managed to get the schema info from the physical files(ibd for 8.x)
The ibd2sdi only takes sdi from a ibd file, this lib can take it from memory and convert the sdi to DDL.

## How is the compatability

## How is the performance

## How to use it

Just give the parser filepath and file reader of the frm file.

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
