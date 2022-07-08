package main

/*
 * 2つのテーブル間の比較を行う.
 *
 * DROP TABLE IF EXISTS bak_hoges
 * CREATE TABLE bak_hoges (SELECT * FROM hoges);
 * ....
 * ....
 * sqldiff -dsn ~/dbinfo.json -table1 bak_hoges -table2 hoges
 *
 *
 * 復帰値
 *   0: 一致
 *   1: 不一致
 *   2: エラー
 */

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type option struct {
	column   string
	modified bool
	header   bool
	fp       io.Writer
	driver   string
	dsn      string
	table1   string
	table2   string
	nrow     int
}

//////////////////////////////////////////////////////////
// 主処理
//////////////////////////////////////////////////////////
func toString(v interface{}, ct *sql.ColumnType) string {
	switch ct.ScanType().String() {
	case "sql.RawBytes", "sql.NullTime":
		w := v.(*sql.RawBytes)
		if w == nil {
			return "null"
		}
		return string(*w)
	case "sql.NullInt64":
		w := v.(*sql.NullInt64)
		if w.Valid {
			return fmt.Sprintf("%d", w.Int64)
		} else {
			return "null"
		}
	case "sql.NullInt32":
		w := v.(*sql.NullInt32)
		if w.Valid {
			return fmt.Sprintf("%d", w.Int32)
		} else {
			return "null"
		}
	case "float32":
		return fmt.Sprintf("%e", *v.(*float32))
	case "float64":
		return fmt.Sprintf("%e", *v.(*float64))
	case "int8":
		return fmt.Sprintf("%d", *v.(*int8))
	case "int32":
		return fmt.Sprintf("%d", *v.(*int32))
	case "uint32":
		return fmt.Sprintf("%v", *v.(*uint32))
	case "int64":
		return fmt.Sprintf("%d", *v.(*int64))
	}
	panic("unsupported: " + ct.ScanType().String())
}

func (opt *option) printHeader() {
	if opt.header {
		fmt.Printf("--- %s\n", opt.table1)
		fmt.Printf("+++ %s\n", opt.table2)
		opt.header = false
	}
}

func (opt *option) printRow(prefix string, row []interface{}) error {
	if opt.nrow == 0 && opt.header {
		opt.printHeader()
	}
	opt.nrow++

	id := *row[0].(*int32)
	fmt.Printf("%s@@ id=%d\n", prefix, id)
	return nil
}
func (opt *option) isSkip(col string) bool {
	return opt.modified && (col == "created" || col == "modified" || col == "created_user" || col == "modified_user")
}

/*
 * return:
 *   true: 一致
 *   false: 不一致
 */
func diff(table1, table2 string, opt option) (bool, error) {
	db, err := sql.Open(opt.driver, opt.dsn)
	if err != nil {
		return false, err
	}
	defer db.Close()

	sql1 := fmt.Sprintf("SELECT %s FROM %s ORDER BY id", opt.column, table1)
	rows1, err := db.Query(sql1)
	if err != nil {
		fmt.Printf("query failed: %s\n", sql1)
		return false, err
	}
	defer rows1.Close()

	sql2 := fmt.Sprintf("SELECT %s FROM %s ORDER BY id", opt.column, table2)
	rows2, err := db.Query(sql2)
	if err != nil {
		fmt.Printf("query failed: %s\n", sql2)
		return false, err
	}
	defer rows2.Close()

	colnames, err := rows1.Columns()
	if err != nil {
		fmt.Printf("columns failed: %s\n", sql1)
		return false, err
	}

	coltypes, err := rows1.ColumnTypes()
	if err != nil {
		fmt.Printf("coltype failed: %s\n", sql1)
		return false, err
	}

	v1 := make([]interface{}, len(colnames))
	v2 := make([]interface{}, len(colnames))
	for i, ct := range coltypes {
		//tp := ct.DatabaseTypeName()
		//		fmt.Printf("[%2d] %20s %20s %20s\n", i, ct.Name(), tp, ct.ScanType())
		//		prec, scale, ok := ct.DecimalSize()
		//		fmt.Printf("decimal=(%d, %d, %v)\n", prec, scale, ok)
		switch ct.ScanType().String() {
		case "sql.RawBytes":
			v1[i] = new(sql.RawBytes)
			v2[i] = new(sql.RawBytes)
		case "sql.NullInt64":
			v1[i] = new(sql.NullInt64)
			v2[i] = new(sql.NullInt64)
		case "sql.NullTime":
			v1[i] = new(sql.RawBytes)
			v2[i] = new(sql.RawBytes)
		case "float32":
			v1[i] = new(float32)
			v2[i] = new(float32)
		case "int8":
			v1[i] = new(int8)
			v2[i] = new(int8)
		case "int32":
			v1[i] = new(int32)
			v2[i] = new(int32)
		case "uint32":
			v1[i] = new(uint32)
			v2[i] = new(uint32)
		default:
			fmt.Printf("type=%v, %s\n", ct.ScanType(), ct.ScanType().String())
			panic("unsupported")
		}
	}

	if true {
		opt.nrow = 0

		for i := 0; ; i++ {
			if !rows1.Next() {
				if rows2.Next() {
					err = rows2.Scan(v2...)
					if err != nil {
						fmt.Printf("row2 scan failed. i=%d\n", i)
						return false, err
					}
					opt.printRow("+", v2)
					continue
				}
				break
			} else if !rows2.Next() {
				err = rows1.Scan(v1...)
				if err != nil {
					fmt.Printf("row1 scan failed. i=%d\n", i)
					return false, err
				}
				opt.printRow("+", v1)
				continue
			}

			err = rows1.Scan(v1...)
			if err != nil {
				fmt.Printf("row1 scan failed. i=%d\n", i)
				return false, err
			}
			err = rows2.Scan(v2...)
			if err != nil {
				fmt.Printf("row2 scan failed. i=%d\n", i)
				return false, err
			}

			updated := false
			// fmt.Printf("id=%d\n", *v1[0].(*int32))
			for j := 0; j < len(v1); j++ {
				if opt.isSkip(colnames[j]) {
					continue
				}
				s1 := toString(v1[j], coltypes[j])
				s2 := toString(v2[j], coltypes[j])
				if s1 != s2 {
					opt.printHeader()
					updated = true
					fmt.Printf("    id=(%4d,%4d), %20s=(%s,%s)\n", *v1[0].(*int32), *v2[0].(*int32), colnames[j], s1, s2)
				}
			}
			if updated {
				opt.nrow++
			}
		}
		if opt.nrow > 0 {
			fmt.Printf("      %d rows are found\n", opt.nrow)
		}
	}

	return opt.nrow == 0, nil
}

//////////////////////////////////////////////////////////
// コマンドライン
//////////////////////////////////////////////////////////

//------------------------------------------
// -dsn option
//------------------------------------------
type dbinfo struct {
	Database string `json:"database"`
	User     string `json:"user"`
	Passwd   string `json:"passwd"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
}

func Json2Dsn(fname string) (string, error) {
	bytes, err := ioutil.ReadFile(fname)
	if err != nil {
		return "", err
	}

	var db dbinfo
	err = json.Unmarshal(bytes, &db)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", db.User, db.Passwd, db.Host, db.Port, db.Database), nil
}

/**
 * data source として適切か?
 */
func isDsnFormat(dsn string) bool {
	pattern := `^[a-z0-9]+:.*@tcp\([a-z0-9_.-]+:[0-9]+\)/[a-zA-Z0-9_.-]+$`
	matched, _ := regexp.MatchString(pattern, dsn)
	return matched
}

/**
 * *.json が指定されたら，ファイル解析し，
 * それ以外だったら dsn 形式かチェックする
 */
func parseDsnOption(opt string) (string, error) {
	dsn := opt
	if strings.HasSuffix(opt, ".json") {
		d, err := Json2Dsn(opt)
		if err != nil {
			return "", err
		}
		dsn = d
	}
	if !isDsnFormat(dsn) {
		return "", fmt.Errorf("invalid format: %s", dsn)
	}
	return dsn, nil
}

func usage(str string) {
	if str != "" {
		fmt.Fprintf(os.Stderr, str)
	}
	flag.Usage()
	os.Exit(1)
}

//////////////////////////////////////////////////////////
// main
//////////////////////////////////////////////////////////
func main() {
	var (
		driver   = flag.String("driver", "mysql", "database driver name. see https://pkg.go.dev/database/sql#Open")
		dsnop    = flag.String("dsn", "", "Data Source Name: user:pass@protocol(ip:port)/db or dbinfo.json. see https://pkg.go.dev/database/sql#Open")
		table1   = flag.String("table1", "", "table name WITH where query")
		table2   = flag.String("table2", "", "table name WITH where query")
		column   = flag.String("column", "*", "default *")
		modified = flag.Bool("modified", false, "except created, created_user, modified, modified_user")
		header   = flag.Bool("p", false, "print")
	)

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: "+os.Args[0]+" -dsn dsn [-p prefix]")
		flag.PrintDefaults()
	}
	flag.Parse()

	dsn, err := parseDsnOption(*dsnop)
	if err != nil {
		usage("-dsn invalid\n")
		return
	}

	var opt option
	opt.column = *column
	opt.modified = *modified
	opt.fp = os.Stdout
	opt.driver = *driver
	opt.dsn = dsn
	opt.header = *header
	opt.table1 = *table1
	opt.table2 = *table2

	ret, err := diff(*table1, *table2, opt)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(2)
	}
	if ret {
		os.Exit(0)
	} else {
		os.Exit(1)
	}
}
