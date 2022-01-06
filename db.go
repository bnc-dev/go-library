package github.com/bnc-dev/go-library

import (
	// "errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	// "log"
	"strings"
	"time"

	sql "database/sql"
	_ "github.com/alexbrainman/odbc"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	// "github.com/go-xorm/xorm"
	"xorm.io/xorm"
)

type DB struct {
	engine           *xorm.Engine
	IsConnected      bool
	IFNULL           string
	FormatToChar     string
	LongFormatToChar string
	Localize         string
	dbConnection     string
	dbName     		string
	connString     string
	dbETL     	   *sql.DB
}

func (this *DB) Init(dbConnection string, dbUser string, dbPass string, dbHost string, dbPort string, dbName string) {
	this.dbConnection = dbConnection
	this.dbName = dbName
	var connString string = ""
	if dbConnection == "mysql" {
		connString = fmt.Sprintf("%s:%s@(%s:%s)/%s?charset=utf8&allowAllFiles=true", dbUser, dbPass, dbHost, dbPort, dbName)
	} else if dbConnection == "mssql" {
		connString = fmt.Sprintf("driver={SQL Server};server=%s;user id=%s;password=%s;database=%s;", dbHost, dbUser, dbPass, dbName)
	} else if dbConnection == "postgres" {
		connString = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", dbUser, dbPass, dbHost, dbName)
	} else if dbConnection == "presto" {
		connString = fmt.Sprintf("https://%s:%s@%s:%s?catalog=default&schema=%s", dbUser, dbPass, dbHost, dbPort, dbName)
}
	this.connString = connString
	engine, err := xorm.NewEngine(dbConnection, connString)

	// engine, err := xorm.NewEngine(dbConnection, dbUser+":"+dbPass+"@("+dbHost+":"+dbPort+")/"+dbName+"?charset=utf8")
	if err != nil {
		// log.Fatal("Error db connection: ", err)
		fmt.Println("Error db connection: ", err)
		this.IsConnected = false
	} else {
		this.engine = engine
		loc, err := time.LoadLocation("Asia/Jakarta")
		if err == nil {
			this.engine.SetTZDatabase(loc)
			this.engine.SetTZLocation(loc)
		}
		this.IsConnected = true

		if dbConnection == "oci8" {
			this.IFNULL = "NVL"
		} else if dbConnection == "mssql" || dbConnection == "sqlsrv" {
			this.IFNULL = "ISNULL"
		} else if dbConnection == "postgres" {
			this.IFNULL = "COALESCE"
		} else {
			this.IFNULL = "IFNULL"
		}

		if dbConnection == "oci8" || dbConnection == "postgres" {
			this.FormatToChar = "TO_CHAR({input},'DD/MM/YYYY')"
			this.LongFormatToChar = "TO_CHAR({input},'DD/MM/YYYY HH24:MI:SS')"
		} else if dbConnection == "mssql" || dbConnection == "sqlsrv" {
			this.FormatToChar = "CONVERT(VARCHAR(10),{input},103)"
			this.LongFormatToChar = "CONVERT(VARCHAR(10), '{input}', 103) + ' '  + CONVERT(VARCHAR(8), '{input}', 14)"
		} else {
			this.FormatToChar = "date_format({input},'%d/%m/%Y')"
			this.LongFormatToChar = "date_format({input},'%d/%m/%Y %H:%i:%s')"
		}
	}
}

func (this *DB) SetEngine(engine *xorm.Engine) {
	this.engine = engine
}

func (this *DB) Close() {
	if this.IsConnected {
		this.engine.Close()
	}
}

func (this *DB) DbfToChar(as_input string) string {
	return strings.Replace(this.FormatToChar, "{input}", as_input, -1)
}

func (this *DB) DbfToLongChar(as_input string) string {
	return strings.Replace(this.LongFormatToChar, "{input}", as_input, -1)
}

/*
func (this *DB) DbfToDate($as_input, $as_format='short'){
	if(FormatToDate == "'{input}'"){
		if($as_format=='short'){
			$as_input=date_format3($as_input,'YYYY-MM-DD');
		}else{
			$as_input=date_format3($as_input,'YYYY-MM-DD HH:MI:SS');
		}
	}else{ $as_input=date_format3($as_input,'DD/MM/YYYY'); }
	return str_replace('{input}',$as_input,($as_format=='short' ? FormatToDate : LongFormatToDate));
}*/

func (this *DB) TableExists(table_name string) (bool, error) {
	var final_result bool = false
	var final_err error
	if this.IsConnected {
		var ls_sql string = ""
		if this.dbConnection == "mysql" {
			ls_sql = `	SELECT	COUNT(1) num_row
						FROM	INFORMATION_SCHEMA.TABLES
						WHERE	TABLE_SCHEMA = '` + this.dbName + `'
								AND TABLE_NAME = '` + table_name + `'`
		} else if this.dbConnection == "mssql" {
			ls_sql = `	SELECT	COUNT(1) num_row
						FROM	INFORMATION_SCHEMA.TABLES
						WHERE	TABLE_CATALOG = '` + this.dbName + `'
								AND TABLE_NAME = '` + table_name + `'`
		} else if this.dbConnection == "postgres" {
			ls_sql = `	SELECT  COUNT(1) num_row
						FROM	INFORMATION_SCHEMA.TABLES
						WHERE	table_catalog = '` + this.dbName + `'
								AND TABLE_NAME = '` + table_name + `'
						ORDER BY table_type, schema_name, table_name`
		} else if this.dbConnection == "presto" {
			ls_sql = `	SELECT	COUNT(1) num_row
						FROM	information_schema.tables
						WHERE	table_schema = '`+ this.dbName + `'
								AND TABLE_NAME = '` + table_name + `'
						ORDER BY table_name`
		}
		res, err := this.engine.QueryString(ls_sql)
		if err != nil {
			// log.Fatal("Error sql statement: ", err)
			fmt.Println("Error sql statement: ", err)
			final_err = err
		} else if(len(res) > 0 && ToInteger64(res[0]["num_row"],0) > 0){
			final_result = true
		}
	}
	return final_result, final_err
}

func (this *DB) Query(param ...interface{}) ([]map[string]string, error) {
	var final_result []map[string]string
	var final_err error
	if this.IsConnected {
		res, err := this.engine.QueryString(param...)
		if err != nil {
			// log.Fatal("Error sql statement: ", err)
			fmt.Println("Error sql statement: ", err)
			final_err = err
		} else {
			final_result = res
		}
	}
	return final_result, final_err
}

func (this *DB) QueryRows(as_sql string) (*sql.Rows, error) {
	var final_result *sql.Rows
	var final_err error

	if this.IsConnected {
		DB_ETL, err := sql.Open(this.dbConnection, this.connString)
		defer DB_ETL.Close()
		if(err != nil){
			fmt.Println("DB Connection, Error >> " + err.Error())
			final_err = err
		}else{
			this.dbETL = DB_ETL
			rows, err := DB_ETL.Query(as_sql)
			if(err != nil){
				fmt.Println("SQL Query, Error >> " + err.Error())
				final_err = err
			}else{
				final_result = rows
			}
		}
	}

	return final_result, final_err
}

func (this *DB) FetchRows(rows *sql.Rows, f_process_each_row func(index_no int64, arr_column []string, map_rows map[string]string, err error)(bool)){ // ([]string, map[string]string, error) {
	var lb_continue = true
	columns, _ := rows.Columns()

	var itter_row int64 = 0
	for rows.Next() {

		var row_error error

		if(!lb_continue){
			fmt.Println("Break not continue")
			break
		}

		map_rows :=  make( map[string]string)
		data_row, err := row2mapStr(rows, columns)
		if(err != nil){
			fmt.Println(err)
			break
		}else{
			map_rows = data_row
		}

		if(lb_continue){
			lb_continue = f_process_each_row(itter_row, columns, map_rows, row_error)
		}

		itter_row++
	}

	this.dbETL.Close()
}

func row2mapStr(rows *sql.Rows, fields []string) (resultsMap map[string]string, err error) {
	result := make(map[string]string)
	scanResultContainers := make([]interface{}, len(fields))
	for i := 0; i < len(fields); i++ {
		var scanResultContainer interface{}
		scanResultContainers[i] = &scanResultContainer
	}
	if err := rows.Scan(scanResultContainers...); err != nil {
		return nil, err
	}

	for ii, key := range fields {
		rawValue := reflect.Indirect(reflect.ValueOf(scanResultContainers[ii]))
		// if row is null then as empty string
		if rawValue.Interface() == nil {
			result[key] = ""
			continue
		}

		if data, err := value2String(&rawValue); err == nil {
			result[key] = data
		} else {
			return nil, err
		}
	}
	return result, nil
}

var (
	emptyString       string
	boolDefault       bool
	byteDefault       byte
	complex64Default  complex64
	complex128Default complex128
	float32Default    float32
	float64Default    float64
	int64Default      int64
	uint64Default     uint64
	int32Default      int32
	uint32Default     uint32
	int16Default      int16
	uint16Default     uint16
	int8Default       int8
	uint8Default      uint8
	intDefault        int
	uintDefault       uint
	timeDefault       time.Time
)

func value2String(rawValue *reflect.Value) (str string, err error) {
	aa := reflect.TypeOf((*rawValue).Interface())
	vv := reflect.ValueOf((*rawValue).Interface())
	switch aa.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		str = strconv.FormatInt(vv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		str = strconv.FormatUint(vv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		str = strconv.FormatFloat(vv.Float(), 'f', -1, 64)
	case reflect.String:
		str = vv.String()
	case reflect.Array, reflect.Slice:
		switch aa.Elem().Kind() {
		case reflect.Uint8:
			data := rawValue.Interface().([]byte)
			str = string(data)
			if str == "\x00" {
				str = "0"
			}
		default:
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	// time type
	case reflect.Struct:
		if aa.ConvertibleTo(reflect.TypeOf(timeDefault)) {
			str = vv.Convert(reflect.TypeOf(timeDefault)).Interface().(time.Time).Format(time.RFC3339Nano)
		} else {
			err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
		}
	case reflect.Bool:
		str = strconv.FormatBool(vv.Bool())
	case reflect.Complex128, reflect.Complex64:
		str = fmt.Sprintf("%v", vv.Complex())
	/* TODO: unsupported types below
	   case reflect.Map:
	   case reflect.Ptr:
	   case reflect.Uintptr:
	   case reflect.UnsafePointer:
	   case reflect.Chan, reflect.Func, reflect.Interface:
	*/
	default:
		err = fmt.Errorf("Unsupported struct type %v", vv.Type().Name())
	}
	return
}

func (this *DB) GetWhere(table string, condition string, param ...interface{}) (map[string]string, error) {
	var final_result map[string]string
	var final_err error
	if this.IsConnected {
		res, err := this.engine.Table(table).Where(condition, param...).QueryString()
		if err != nil {
			// log.Fatal("Error sql statement: ", err)
			fmt.Println("Error sql statement: ", err)
			final_err = err
		} else if len(res) > 0 {
			// 	final_err = errors.New("not found")
			// } else {
			final_result = res[0]
		}
	}
	return final_result, final_err
}

func (this *DB) SqlGetData(param ...interface{}) (string, error) {
	var lsResult string = ""
	final_result, final_err := this.Query(param...)
	if final_err == nil && len(final_result) > 0 {
		for _, val := range final_result[0] {
			lsResult = val
			break
		}
	}
	return lsResult, final_err
}

func (this *DB) Insert(table string, data map[string]interface{}) (sql.Result, error) {
	var final_result sql.Result
	var final_err error
	var lsSQL string = "INSERT INTO " + table
	var lsField string = ""
	var lsValue string = ""
	args := []interface{}{ }
	for key, val := range data { 
		if(lsField != ""){
			lsField += ","
		}
		lsField += key

		if(lsValue != ""){
			lsValue += ","
		}
		lsValue += "?"

		args =  append(args, val) 
	}

	lsSQL += "(" + lsField + ") VALUES (" + lsValue + ")"
	// fmt.Println("lsSQL:insert",lsSQL,args)
	args = append([]interface{}{ lsSQL }, args...)
	res, err := this.engine.Exec(args...)
	if err != nil {
		// log.Fatal("Error sql statement: ", err, lsSQL)
		fmt.Println("Error sql statement: ", err, lsSQL)
		final_err = err
	} else {
		final_result = res
	}

	return final_result, final_err
}

func (this *DB) Update(table string, data map[string]interface{}, criteria map[string]interface{}) (sql.Result, error) {
	var final_result sql.Result
	var final_err error
	var lsSQL string = "UPDATE " + table
	var lsField string = ""
	var lsCriteria string = ""
	args := []interface{}{ }
	for key, val := range data { 
		if(lsField != ""){
			lsField += ", "
		}
		lsField += key + " = ?"

		args =  append(args, val) 
	}

	for key, val := range criteria { 
		if(lsCriteria != ""){
			lsCriteria += " AND "
		}
		lsCriteria += key + " = ?"

		args =  append(args, val) 
	}

	lsSQL += " SET " + lsField + " WHERE " + lsCriteria
	// fmt.Println("lsSQL:update",lsSQL,args)
	args = append([]interface{}{ lsSQL }, args...)
	res, err := this.engine.Exec(args...)
	if err != nil {
		// log.Fatal("Error sql statement: ", err, lsSQL)
		fmt.Println("Error sql statement: ", err, lsSQL)
		final_err = err
	} else {
		final_result = res
	}

	return final_result, final_err
}

func (this *DB) Delete(table string, criteria map[string]interface{}) (sql.Result, error) {
	var final_result sql.Result
	var final_err error
	var lsSQL string = "DELETE FROM " + table
	var lsCriteria string = ""
	args := []interface{}{ }
	for key, val := range criteria { 
		if(lsCriteria != ""){
			lsCriteria += " AND "
		}
		lsCriteria += key + " = ?"

		args =  append(args, val) 
	}

	lsSQL += " WHERE " + lsCriteria
	// fmt.Println("lsSQL:delete",lsSQL,args)
	args = append([]interface{}{ lsSQL }, args...)
	res, err := this.engine.Exec(args...)
	if err != nil {
		// log.Fatal("Error sql statement: ", err, lsSQL)
		fmt.Println("Error sql statement: ", err, lsSQL)
		final_err = err
	} else {
		final_result = res
	}

	return final_result, final_err
}

func (this *DB) Exec(param ...interface{}) (sql.Result, error) {
	var final_result sql.Result
	var final_err error
	
	res, err := this.engine.Exec(param...)
	if err != nil {
		fmt.Println("Error sql statement: ", err, param)
		final_err = err
	} else {
		final_result = res
	}

	return final_result, final_err
}

type VPaging struct {
	Page       int64        `json:"page"`
	NumPage    int64        `json:"num_page"`
	Total      int64        `json:"total"`
	RowPerPage int64        `json:"row_per_page"`
	Rows       interface{}  `json:"rows"`
	// RowsTmp    []orm.Params `json:"-"`
}

func (this *DB) Paging(asSQL string, aiPage int64, aiPageSize int64) (bool, string, VPaging) {
	var lbError = false
	var lsError = ""
	var paging VPaging

	// Explode SQL Statement into array [SELECT, FROM, WHERE, ORDER BY]
	var tmp string = ""
	var itter_after_reset int64 = 0
	var itter_bracket int64 = 0
	var keyword_position string = ""
	var arr_word []string
	var arr_keyword_query map[string]string = map[string]string{"SELECT": "", "FROM": "", "WHERE": "", "ORDER BY": ""}
	var arr_keyword_select map[string]string = map[string]string{}
	var arr_keyword_order map[string]string = map[string]string{}
	//var arr_word_select []string
	var word_select string = ""
	var word_order string = ""
	var word_orders []string
	var field_alias string = ""
	var field_source string = ""
	var sort string = ""
	var st_cutword bool = false
	var word string = ""

	for i, char := range asSQL {
		_ = i
		if char == '(' {
			itter_bracket++
		} else if char == ')' {
			itter_bracket--
		}

		if string(char) == " " || string(char) == "\t" || string(char) == "\n" {
			if itter_after_reset == 0 {
				word = tmp
				arr_word = append(arr_word, word)
			}
			itter_after_reset++
			tmp = ""
			st_cutword = true
		} else {
			word = ""
			tmp += string(char)
			itter_after_reset = 0
			st_cutword = false
		}
		
		if strings.ToUpper(word) == "SELECT" && itter_bracket == 0 {
			keyword_position = "SELECT"
		} else if strings.ToUpper(word) == "FROM" && itter_bracket == 0 {
			keyword_position = "FROM"
		} else if strings.ToUpper(word) == "WHERE" && itter_bracket == 0 {
			keyword_position = "WHERE"
		} else if strings.ToUpper(word) == "BY" && strings.ToUpper(arr_word[len(arr_word)-2]) == "ORDER" && itter_bracket == 0 {
			keyword_position = "ORDER BY"
		}
		
		if keyword_position == "SELECT" {
			if (st_cutword && itter_after_reset <= 1) || !st_cutword {
				arr_keyword_query["SELECT"] += string(char)
			}
			if string(char) == "," && itter_bracket == 0 {

				var arr_tmp_field []string = strings.Fields(strings.TrimSpace(word_select))
				field_alias = ""
				field_source = ""

				if len(arr_tmp_field) > 1 {
					for j, tmp_field := range arr_tmp_field {
						if j == len(arr_tmp_field)-1 {
							field_alias = tmp_field
						} else {
							field_source += " " + tmp_field
						}
					}
				} else if len(arr_tmp_field) == 1 {
					arr_tmp_field = strings.Split(strings.TrimSpace(word_select), ".")
					if len(arr_tmp_field) == 2 {
						field_alias = arr_tmp_field[1]
						field_source = word_select
					} else {
						field_alias = word_select
						field_source = word_select
					}
				}

				arr_keyword_select[field_alias] = strings.TrimSpace(field_source)
				// fmt.Println("--------------------")
				// fmt.Println(i)
				// fmt.Println(strings.TrimSpace(word_select))
				// fmt.Println(field_alias)
				// fmt.Println(field_source)
				// fmt.Println(arr_keyword_select)
				word_select = ""
			} else {
				word_select += string(char)
			}
		} else if keyword_position == "FROM" && ((st_cutword && itter_after_reset <= 1) || !st_cutword) {
			arr_keyword_query["FROM"] += string(char)
		} else if keyword_position == "WHERE" && ((st_cutword && itter_after_reset <= 1) || !st_cutword) {
			arr_keyword_query["WHERE"] += string(char)
		} else if keyword_position == "ORDER BY" {
			if (st_cutword && itter_after_reset <= 1) || !st_cutword {
				arr_keyword_query["ORDER BY"] += string(char)
			}
			if string(char) == "," && itter_bracket == 0 {

				word_order = strings.TrimSpace(word_order)
				field_alias = ""
				field_source = ""
				sort = "ASC"

				if len(word_order) > 5 && strings.ToUpper(word_order[len(word_order)-5:len(word_order)]) == " DESC" {
					sort = "DESC"
					word_order = word_order[0 : len(word_order)-5]
				} else if len(word_order) > 4 && strings.ToUpper(word_order[len(word_order)-4:len(word_order)]) == " ASC" {
					sort = "ASC"
					word_order = word_order[0 : len(word_order)-4]
				}

				var arr_tmp_field []string = strings.Split(strings.TrimSpace(word_order), ".")
				if !strings.Contains(word_order, "(") && !strings.Contains(word_order, ")") && len(arr_tmp_field) == 2 {
					field_alias = arr_tmp_field[1]
					field_source = word_order
				} else {
					field_alias = word_order
					field_source = word_order
				}

				field_alias = strings.TrimSpace(field_alias)
				field_source = strings.TrimSpace(field_source) + " " + sort
				arr_keyword_order[field_alias] = field_source
				if !sliceExists(word_orders, field_alias) {
					word_orders = append(word_orders, field_alias)
				}
				// fmt.Println("--------------------")
				// fmt.Println(i)
				// fmt.Println(strings.TrimSpace(word_order))
				// fmt.Println(field_alias)
				// fmt.Println(field_source)
				word_order = ""
			} else {
				word_order += string(char)
			}
		}

	}

	arr_keyword_query["SELECT"] = strings.TrimSpace(arr_keyword_query["SELECT"])
	arr_keyword_query["FROM"] = strings.TrimSpace(arr_keyword_query["FROM"])
	arr_keyword_query["WHERE"] = strings.TrimSpace(arr_keyword_query["WHERE"])
	arr_keyword_query["ORDER BY"] = strings.TrimSpace(arr_keyword_query["ORDER BY"])
	
	// fmt.Println("SELECT:", arr_keyword_query["SELECT"])
	// fmt.Println("FROM:",arr_keyword_query["FROM"])
	// fmt.Println("WHERE:",arr_keyword_query["WHERE"])
	// fmt.Println("ORDER BY:",arr_keyword_query["ORDER BY"])
	
	arr_keyword_query["SELECT"] = strings.TrimSpace(arr_keyword_query["SELECT"][0 : len(arr_keyword_query["SELECT"])-4])
	if arr_keyword_query["WHERE"] != "" {
		arr_keyword_query["FROM"] = strings.TrimSpace(arr_keyword_query["FROM"][0 : len(arr_keyword_query["FROM"])-5])
	}
	if arr_keyword_query["WHERE"] != "" && arr_keyword_query["ORDER BY"] != "" {
		arr_keyword_query["WHERE"] = strings.TrimSpace(arr_keyword_query["WHERE"][0 : len(arr_keyword_query["WHERE"])-8])
	}
	if arr_keyword_query["FROM"] != "" && arr_keyword_query["WHERE"] == "" && arr_keyword_query["ORDER BY"] != "" {
		arr_keyword_query["FROM"] = strings.TrimSpace(arr_keyword_query["FROM"][0 : len(arr_keyword_query["FROM"])-8])
	}
	if arr_keyword_query["ORDER BY"] != "" && 1==2 { // skip this, because it makes [ORDER BY] blank
		// Patch Last Field in SELECT part
		word_select = strings.TrimSpace(word_select)
		var arr_tmp_field []string = strings.Fields(word_select[0 : len(word_select)-4])
		field_alias = ""
		field_source = ""

		if len(arr_tmp_field) > 1 {
			for j, tmp_field := range arr_tmp_field {
				if j == len(arr_tmp_field)-1 {
					field_alias = tmp_field
				} else {
					field_source += " " + tmp_field
				}
			}
		} else if len(arr_tmp_field) == 1 {
			arr_tmp_field = strings.Split(strings.TrimSpace(word_select), ".")
			if len(arr_tmp_field) == 2 {
				field_alias = arr_tmp_field[1]
				field_source = word_select
			} else {
				field_alias = word_select
				field_source = word_select
			}
		}

		arr_keyword_select[field_alias] = strings.TrimSpace(field_source)
		// fmt.Println(arr_keyword_select)

		// Patch Last Field in ORDER BY part
		word_order = strings.TrimSpace(word_order)
		field_alias = ""
		field_source = ""
		sort = "ASC"

		if len(word_order) > 5 && strings.ToUpper(word_order[len(word_order)-5:len(word_order)]) == " DESC" {
			sort = "DESC"
			word_order = word_order[0 : len(word_order)-5]
		} else if len(word_order) > 4 && strings.ToUpper(word_order[len(word_order)-4:len(word_order)]) == " ASC" {
			sort = "ASC"
			word_order = word_order[0 : len(word_order)-4]
		}

		arr_tmp_field = strings.Split(strings.TrimSpace(word_order), ".")
		if !strings.Contains(word_order, "(") && !strings.Contains(word_order, ")") && len(arr_tmp_field) == 2 {
			field_alias = arr_tmp_field[1]
			field_source = word_order
		} else {
			field_alias = word_order
			field_source = word_order
		}

		field_alias = strings.TrimSpace(field_alias)
		field_source = strings.TrimSpace(field_source) + " " + sort
		arr_keyword_order[field_alias] = field_source
		if !sliceExists(word_orders, field_alias) {
			word_orders = append(word_orders, field_alias)
		}

		// Validate OrderBy
		var lsOrderBy string = ""
		for i, field_alias := range word_orders {
			_ = i
			field_source = arr_keyword_order[field_alias]
			if strings.Contains(field_source, "(") && strings.Contains(field_source, ")") {
				if lsOrderBy != "" {
					lsOrderBy += ", "
				}
				lsOrderBy += field_source
			} else if _, ok := arr_keyword_select[field_alias]; ok {
				if lsOrderBy != "" {
					lsOrderBy += ", "
				}
				lsOrderBy += field_source
			}
		}
		arr_keyword_query["ORDER BY"] = lsOrderBy

	}
	// fmt.Println("--------------")
	// fmt.Println("SELECT:", arr_keyword_query["SELECT"])
	// fmt.Println("FROM:",arr_keyword_query["FROM"])
	// fmt.Println("WHERE:",arr_keyword_query["WHERE"])
	// fmt.Println("ORDER BY:",arr_keyword_query["ORDER BY"])
	
	var ls_sql_total = "SELECT COUNT(1) jml FROM " + arr_keyword_query["FROM"] + " "
	if arr_keyword_query["WHERE"] != "" {
		ls_sql_total += " WHERE " + arr_keyword_query["WHERE"]
	}
	
	var liTotal int64 = 0
	res, err := this.engine.QueryString(ls_sql_total)
	if err != nil {
		fmt.Println("Paging Count:", err.Error())
	}else if(res != nil && len(res)> 0){
		liTotal, _ = strconv.ParseInt((res[0]["jml"]), 10, 64)
	}
	
	paging.Total = liTotal
	paging.RowPerPage = aiPageSize
	
	var lnCurrentPage int64 = aiPage
	if aiPageSize < 1 {
		aiPageSize = 1
	}
	
	paging.NumPage = int64(math.Ceil(float64(paging.Total) / float64(aiPageSize)))
	if lnCurrentPage > paging.NumPage {
		lnCurrentPage = paging.NumPage
	}
	if lnCurrentPage < 1 {
		lnCurrentPage = 1
	}
	paging.Page = lnCurrentPage
	
	// Set Start & End Rows
	var liStart int64 = ((lnCurrentPage - 1) * aiPageSize) + 1
	var liEnd int64 = liStart + aiPageSize - 1

	// SQL Paging
	var ls_sql_with_paging string = ""

	// --- MySQL / PostgreSQL
	if(this.dbConnection == "mysql" || this.dbConnection == "postgres"){
		ls_sql_with_paging = `SELECT	` + arr_keyword_query["SELECT"] + " \n" + `FROM	` + arr_keyword_query["FROM"]
		if strings.TrimSpace(arr_keyword_query["WHERE"]) != "" {
			ls_sql_with_paging += "\n" + ` WHERE ` + arr_keyword_query["WHERE"]
		}
		if strings.TrimSpace(arr_keyword_query["ORDER BY"]) != "" {
			ls_sql_with_paging += " \n" + `ORDER BY ` + arr_keyword_query["ORDER BY"]
		}
		ls_sql_with_paging += " \n" + `LIMIT ` + strconv.FormatInt((lnCurrentPage-1)*aiPageSize, 10) + `, ` + strconv.FormatInt(aiPageSize, 10)
	}
	// var ls_sql_with_paging string = `SELECT	` + arr_keyword_query["SELECT"] +
	// 	" \n" + `FROM	` + arr_keyword_query["FROM"]
	// if strings.TrimSpace(arr_keyword_query["WHERE"]) != "" {
	// 	ls_sql_with_paging += "\n" + ` WHERE ` + arr_keyword_query["WHERE"]
	// }
	// if strings.TrimSpace(arr_keyword_query["ORDER BY"]) != "" {
	// 	ls_sql_with_paging += " \n" + `ORDER BY ` + arr_keyword_query["ORDER BY"]
	// }
	// ls_sql_with_paging += " \n" + `LIMIT ` + strconv.FormatInt((lnCurrentPage-1)*aiPageSize, 10) + `, ` + strconv.FormatInt(aiPageSize, 10)

	// --- SQL Server
	if(this.dbConnection == "mssql"){
			/*var lsOffset string = strconv.FormatInt(((lnCurrentPage-1)*aiPageSize) + 1, 10) 
		var lsLimit string = strconv.FormatInt(aiPageSize, 10)
		ls_sql_with_paging = `WITH PagingResult AS
			(
				SELECT	` + arr_keyword_query["SELECT"] + `,
						ROW_NUMBER() OVER (ORDER BY ` + arr_keyword_query["ORDER BY"] + `) AS RowNum
				FROM	` + arr_keyword_query["FROM"] + `
				WHERE	` + arr_keyword_query["WHERE"] + `
			)
			SELECT *
			FROM PagingResult
			WHERE RowNum >= ` + lsOffset + `
			AND RowNum <= ` + lsOffset + ` + ` + lsLimit + ` - 1
			ORDER BY RowNum`*/

		
		offset := (aiPage - 1) * aiPageSize
		ls_offset := strconv.Itoa(int(offset))
		ls_page_size := strconv.Itoa(int(aiPageSize))
		ls_sql_with_paging = `SELECT	` + arr_keyword_query["SELECT"] + `
							  FROM	` + arr_keyword_query["FROM"] + `
							  WHERE	` + arr_keyword_query["WHERE"] + `
							  ORDER BY ` + arr_keyword_query["ORDER BY"] + `
							  OFFSET ` + ls_offset + ` ROWS FETCH NEXT ` + ls_page_size + ` ROWS ONLY `
	}
	
	// fmt.Println("----- >> SELECT : ")
	// fmt.Println(arr_keyword_query["SELECT"])
	// fmt.Println("----- >> FROM : ")
	// fmt.Println(arr_keyword_query["FROM"])
	// fmt.Println("----- >> WHERE : ")
	// fmt.Println(arr_keyword_query["WHERE"])
	// fmt.Println("----- >> ORDER BY : ")
	// // fmt.Println(arr_keyword_query["ORDER BY"])
	// fmt.Println("ls_sql_with_paging : ")
	// fmt.Println(ls_sql_with_paging)
	
	// Get data with Paging
	rows, err := this.engine.QueryString(ls_sql_with_paging)
	
	if err == nil {
		if(len(rows) > 0){
			paging.Rows = rows
		}else{
			paging.Rows = []string{ }
		}
	} else {
		//fmt.Println(err)
		paging.Rows = []string{ }
		lbError = true
		lsError = err.Error()
	}

	// _ = arr_keyword_select
	// _ = sort
	// _ = order
	// _ = liTotal
	// _ = liStart
	_ = liEnd
	// _ = tmp
	// _ = itter_after_reset
	// _ = itter_bracket
	// _ = keyword_position
	// _ = arr_word
	// _ = arr_keyword_query
	// _ = st_cutword
	// _ = paging

	return lbError, lsError, paging
}

func sliceExists(slice interface{}, item interface{}) bool {
	s := reflect.ValueOf(slice)

	if s.Kind() != reflect.Slice {
		return false
		// panic("SliceExists() given a non-slice type")
	}

	for i := 0; i < s.Len(); i++ {
		if s.Index(i).Interface() == item {
			return true
		}
	}

	return false
}

// func (this *DB) GetSystemMessage(code string) string {
// 	var lsResult string = ""

// 	lsResult = this.Trans(code)

// 	if lsResult == "" {
// 		row, err := this.GetWhere("it_system_message", "code = ?", code)
// 		if err == nil {
// 			lsResult = row["message_"+this.Localize]
// 		}
// 	}
// 	return lsResult
// }

// func (this *DB) ErrorField(field string, code string) map[string]interface{} {
// 	var lsCode string = ""

// 	m :=  make( map[string]interface{})
// 	m["code"] = strconv.ParseInt(code, 10, 64);
// 	m["field"] = field
// 	m["label"] = this.Trans(field)
// 	m["message"] = this.GetSystemMessage(code)

// 	return m
// }

// func (this *DB) Trans(code string) string {
// 	var lsResult string = code
// 	if code == "validation_fail" {
// 		if this.Localize == "id" {
// 			lsResult = "Validasi gagal. Silahkan cek inputan."
// 		} else {
// 			lsResult = "Validation failed. Please check your input."
// 		}
// 	}
// 	return lsResult
// }
// }
