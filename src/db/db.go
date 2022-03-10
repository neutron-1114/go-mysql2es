package db

import (
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql2es/src/config"
	"strings"
)

const Unknown uint8 = 0

const INT uint8 = 1

const BIGINT uint8 = 2

const VARCHAR uint8 = 3

const TINYINT uint8 = 4

const TEXT uint8 = 5

const UnsignedBigint uint8 = 6

const SmallInt uint8 = 7

const UnsignedSmallInt uint8 = 8

type DB struct {
	db               *sql.DB
	rule             *config.Rule
	rangeSearchModel string
	idSearchModel    string
	colls            []*config.Coll
}

func New(conf *config.Conf) *DB {
	mysqlConf := conf.MySQL
	uri := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", mysqlConf.User, mysqlConf.Password, mysqlConf.Host, mysqlConf.Port, mysqlConf.Database)
	db, err := sql.Open("mysql", uri)
	if err != nil {
		log.Panicf("[DB] 连接 %v 失败... %v", uri, err)
	}
	return &DB{db, conf.Rule, "", "", nil}
}

func (d *DB) FillCollType() {
	rule := d.rule
	mainTable := rule.MainTable
	rows, err := d.db.Query(fmt.Sprintf("DESC %v", mainTable.TableName))
	if err != nil {
		log.Panicf("[PREPARE] 获取表元数据 %v 失败... %v", mainTable.TableName, err)
	}
	fullColl := len(rule.MainTable.CollList) == 0
	for rows.Next() {
		var field string
		var t string
		var temp interface{}
		err = rows.Scan(&field, &t, &temp, &temp, &temp, &temp)
		if err != nil {
			log.Panicf("[PREPARE] 获取表元数据 %v 失败... %v", mainTable.TableName, err)
		}
		if fullColl {
			rule.MainTable.CollList[field] = &config.Coll{
				CollName:    field,
				CollType:    GetCollTypeFromMysql(t),
				MappingName: field,
				FullName:    fmt.Sprintf("%v.%v", mainTable.TableName, field),
			}
		} else {
			coll, e := rule.MainTable.CollList[field]
			if e {
				coll.CollType = GetCollTypeFromMysql(t)
				if coll.CollType == Unknown {
					log.Panicf("[PREPARE] %v.%v 类型不支持", rule.MainTable.TableName, field)
				}
			}
		}
	}
	for k, v := range rule.JoinTables {
		rows, err := d.db.Query(fmt.Sprintf("DESC %v", k))
		if err != nil {
			log.Panicf("[PREPARE] 获取表元数据 %v 失败... %v", k, err)
		}
		fullColl = len(v.CollList) == 0
		for rows.Next() {
			var field string
			var t string
			var temp interface{}
			err = rows.Scan(&field, &t, &temp, &temp, &temp, &temp)
			if err != nil {
				log.Panicf("[PREPARE] 获取表元数据 %v 失败... %v", k, err)
			}
			if fullColl {
				v.CollList[field] = &config.Coll{
					CollName:    field,
					CollType:    GetCollTypeFromMysql(t),
					MappingName: field,
					FullName:    fmt.Sprintf("%v.%v", v.TableName, field),
				}
			} else {
				coll, e := v.CollList[field]
				if e {
					coll.CollType = GetCollTypeFromMysql(t)
					if coll.CollType == Unknown {
						log.Panicf("[PREPARE] %v.%v 类型不支持", k, field)
					}
				}
			}
		}
	}
}

func (d *DB) GetIdRange() (uint64, uint64) {
	rows, err := d.db.Query(fmt.Sprintf("SELECT MAX(`%v`), MIN(`%v`) FROM `%v`", d.rule.MainTable.MainCollName, d.rule.MainTable.MainCollName, d.rule.MainTable.TableName))
	if err != nil {
		log.Panicf("[DB] 获取 %v ID范围失败... %v", d.rule.MainTable.MainCollName, err)
	}
	var minId uint64 = 0
	var maxId uint64 = 0
	for rows.Next() {
		err = rows.Scan(&maxId, &minId)
		if err != nil || minId == 0 || maxId == 0 {
			log.Panicf("[DB] 获取 %v ID范围失败... %v", d.rule.MainTable.MainCollName, err)
		}
	}
	return minId, maxId
}

func (d *DB) buildModel() {
	var colls []*config.Coll
	var collsBuilder []string
	for _, v := range d.rule.MainTable.CollList {
		colls = append(colls, v)
		collsBuilder = append(collsBuilder, fmt.Sprintf("`%v`.`%v`", d.rule.MainTable.TableName, v.CollName))
	}
	for _, table := range d.rule.JoinTables {
		for _, v := range table.CollList {
			colls = append(colls, v)
			collsBuilder = append(collsBuilder, fmt.Sprintf("`%v`.`%v`", table.TableName, v.CollName))
		}
	}
	var fromBuilder = fmt.Sprintf("FROM `%v` ", d.rule.MainTable.TableName)
	for _, table := range d.rule.JoinTables {
		fromBuilder += fmt.Sprintf("LEFT JOIN `%v` ON `%v`.`%v` = `%v`.`%v` ", table.TableName, table.TableName, table.JoinCollName, d.rule.MainTable.TableName, table.MainCollName)
	}
	d.colls = colls
	d.rangeSearchModel = fmt.Sprintf("SELECT %v %v WHERE `%v`.`%v`", strings.Join(collsBuilder, ","), fromBuilder, d.rule.MainTable.TableName, d.rule.MainTable.MainCollName) + " BETWEEN %v AND %v"
	d.idSearchModel = fmt.Sprintf("SELECT %v %v WHERE `%v`.`%v`", strings.Join(collsBuilder, ","), fromBuilder, d.rule.MainTable.TableName, d.rule.MainTable.MainCollName) + " in (%v)"
}

func (d *DB) FullGetByRange(startId uint64, endId uint64) []map[*config.Coll]interface{} {
	if d.rangeSearchModel == "" {
		d.buildModel()
	}
	execSQL := fmt.Sprintf(d.rangeSearchModel, startId, endId)
	rows, err := d.db.Query(execSQL)
	if err != nil {
		log.Panicf("%v 执行失败... %v", execSQL, err)
	}
	var resultList []map[*config.Coll]interface{}
	for rows.Next() {
		var params []interface{}
		for _, coll := range d.colls {
			switch coll.CollType {
			case INT, BIGINT, TINYINT, SmallInt:
				var arg int64
				params = append(params, &arg)
			case VARCHAR, TEXT:
				var arg string
				params = append(params, &arg)
			case UnsignedBigint, UnsignedSmallInt:
				var arg uint64
				params = append(params, &arg)
			default:
				fmt.Println(coll)
			}
		}
		err = rows.Scan(params...)
		if err != nil {
			// 忽略空异常
			if !strings.Contains(err.Error(), "converting NULL") {
				log.Panicf("%v 解析失败... %v", execSQL, err)
			}
		}
		result := make(map[*config.Coll]interface{})
		for i, v := range params {
			switch d.colls[i].CollType {
			case INT, BIGINT, TINYINT, SmallInt:
				result[d.colls[i]] = *(v.(*int64))
			case VARCHAR, TEXT:
				result[d.colls[i]] = *(v.(*string))
			case UnsignedBigint, UnsignedSmallInt:
				result[d.colls[i]] = *(v.(*uint64))
			}
		}
		resultList = append(resultList, result)
	}
	return resultList
}

func (d *DB) FullGetById(ids []interface{}) []map[*config.Coll]interface{} {
	if d.idSearchModel == "" {
		d.buildModel()
	}
	var idsStr []string
	for _, id := range ids {
		idsStr = append(idsStr, fmt.Sprintf("%v", id))
	}
	execSQL := fmt.Sprintf(d.idSearchModel, strings.Join(idsStr, ","))
	rows, err := d.db.Query(execSQL)
	if err != nil {
		log.Panicf("%v 执行失败... %v", execSQL, err)
	}
	var resultList []map[*config.Coll]interface{}
	for rows.Next() {
		var params []interface{}
		for _, coll := range d.colls {
			switch coll.CollType {
			case INT, BIGINT, TINYINT, SmallInt:
				var arg int64
				params = append(params, &arg)
			case VARCHAR, TEXT:
				var arg string
				params = append(params, &arg)
			case UnsignedBigint, UnsignedSmallInt:
				var arg uint64
				params = append(params, &arg)
			}
		}
		err = rows.Scan(params...)
		if err != nil {
			// 忽略空异常
			if !strings.Contains(err.Error(), "converting NULL") {
				log.Panicf("%v 解析失败... %v", execSQL, err)
			}
		}
		result := make(map[*config.Coll]interface{})
		for i, v := range params {
			switch d.colls[i].CollType {
			case INT, BIGINT, TINYINT, SmallInt:
				result[d.colls[i]] = *(v.(*int64))
			case VARCHAR, TEXT:
				result[d.colls[i]] = *(v.(*string))
			case UnsignedBigint, UnsignedSmallInt:
				result[d.colls[i]] = *(v.(*uint64))
			}
		}
		resultList = append(resultList, result)
	}
	return resultList
}

func (d *DB) GetMainIdsByJoinId(joinId interface{}, joinTableName string) []interface{} {
	mainFieldName := d.rule.JoinTables[joinTableName].MainCollName
	execSQL := fmt.Sprintf("SELECT `%v` FROM `%v` WHERE `%v` = ",
		d.rule.MainTable.MainCollName,
		d.rule.MainTable.TableName,
		mainFieldName)
	if d.rule.MainTable.CollList[mainFieldName].CollType == VARCHAR ||
		d.rule.MainTable.CollList[mainFieldName].CollType == TEXT {
		execSQL += fmt.Sprintf(`"%v"`, joinId)
	} else {
		execSQL += fmt.Sprintf(`%v`, joinId)
	}
	rows, err := d.db.Query(execSQL)
	if err != nil {
		log.Panicf("%v 执行失败... %v", execSQL, err)
	}
	colls := []*config.Coll{d.rule.MainTable.CollList[mainFieldName]}
	var resultList []interface{}
	for rows.Next() {
		var params []interface{}
		for _, coll := range colls {
			switch coll.CollType {
			case INT, BIGINT, TINYINT, SmallInt:
				var arg int64
				params = append(params, &arg)
			case VARCHAR, TEXT:
				var arg string
				params = append(params, &arg)
			case UnsignedBigint, UnsignedSmallInt:
				var arg uint64
				params = append(params, &arg)
			}
		}
		err = rows.Scan(params...)
		if err != nil {
			// 忽略空异常
			if !strings.Contains(err.Error(), "converting NULL") {
				log.Panicf("%v 解析失败... %v", execSQL, err)
			}
		}
		result := make(map[string]interface{})
		for i, v := range params {
			switch colls[i].CollType {
			case INT, BIGINT, TINYINT, SmallInt:
				result[colls[i].CollName] = *(v.(*int64))
			case VARCHAR, TEXT:
				result[colls[i].CollName] = *(v.(*string))
			case UnsignedBigint, UnsignedSmallInt:
				result[colls[i].CollName] = *(v.(*uint64))
			}
		}
		resultList = append(resultList, result[mainFieldName])
	}
	return resultList
}

func (d *DB) GetOldestBinlogName() string {
	rows, err := d.db.Query("SHOW BINARY LOGS")
	if err != nil {
		log.Panic("获取BINLOG文件失败，停止增量更新")
	} else {
		for rows.Next() {
			var logName string
			var fileSize uint64
			err = rows.Scan(&logName, &fileSize)
			if err != nil {
				log.Panic("获取BINLOG文件失败，停止增量更新")
			} else {
				return logName
			}
		}
	}
	return ""
}

func GetCollTypeFromMysql(t string) uint8 {
	if strings.HasPrefix(t, "int(") {
		return INT
	} else if strings.HasPrefix(t, "tinyint(") {
		return TINYINT
	} else if strings.HasPrefix(t, "bigint(") && strings.HasSuffix(t, "unsigned") {
		return UnsignedBigint
	} else if strings.HasPrefix(t, "bigint(") {
		return BIGINT
	} else if strings.HasPrefix(t, "varchar(") {
		return VARCHAR
	} else if strings.Contains(t, "text") {
		return TEXT
	} else if strings.HasPrefix(t, "smallint(") && strings.HasSuffix(t, "unsigned") {
		return UnsignedSmallInt
	} else if strings.HasPrefix(t, "smallint(") {
		return SmallInt
	} else {
		return Unknown
	}
}
