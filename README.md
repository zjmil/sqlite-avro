# sqlite_avro

This library is a [SQLite Loadable Extension](https://sqlite.org/loadext.html)
for reading [Avro](https://avro.apache.org) files.



### Usage
```
$ sqlite3
> .load avro
> create virtual table mydata.avro using avro('mydata.avro');
> select * from mydata.avro;
```
