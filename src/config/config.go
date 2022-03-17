package config

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"go-mysql2es/src/utils"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"math/rand"
	"os"
	"time"
)

type Conf struct {
	Rebuild    *RebuildConf
	BinLogConf *BinLogConf
	MySQL      *MySQLConf
	ES         *ESConf
	Rule       *Rule
}

type RebuildConf struct {
	Active bool
	Alias  string
	Cron   string
}

type BinLogConf struct {
	StartBinLogName      string
	StartBinLogPosition  int
	BinLogStatusFilePath string
}

type ESConf struct {
	Host  string
	Port  int
	Index string
	Type  string
}

type MySQLConf struct {
	Host              string
	Port              int
	User              string
	Password          string
	Flavor            string
	UseDecimal        bool
	ReadTimeoutMs     int
	HeartbeatPeriodMs int
	ServerID          uint32
	Database          string
}

type Coll struct {
	CollName    string
	CollType    uint8
	MappingName string
	FullName    string
}

type JoinTable struct {
	TableName    string
	CollList     map[string]*Coll
	JoinCollName string
	MainCollName string
}

type MainTable struct {
	TableName    string
	CollList     map[string]*Coll
	MainCollName string
}

type Rule struct {
	JoinTables map[string]*JoinTable
	MainTable  *MainTable
}

func getOrError(m map[interface{}]interface{}, key interface{}, msg string, check func(v interface{}) bool) interface{} {
	v, e := m[key]
	if e && check(v) {
		return v
	} else {
		log.Panic(msg)
	}
	return nil
}

func getOrDefault(m map[interface{}]interface{}, key interface{}, defaultValue interface{}, check func(v interface{}) bool) interface{} {
	v, e := m[key]
	if e && check(v) {
		return v
	} else {
		return defaultValue
	}
}

var NotCheck = func(v interface{}) bool {
	return true
}

func Load(confPath *string) *Conf {
	file, err := os.Open(*confPath)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		_ = file.Close()
	}()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Panic(err)
	}
	m := make(map[string]interface{})
	err = yaml.Unmarshal(content, &m)
	if err != nil {
		log.Panic(err)
	}
	conf := &Conf{}
	conf.initRebuildConf(m["rebuild"].(map[interface{}]interface{}))
	conf.initMySQLConf(m["mysql"].(map[interface{}]interface{}))
	conf.initRule(m["rule"].(map[interface{}]interface{}))
	conf.initESConf(m["es"].(map[interface{}]interface{}))
	conf.initBinLogConf(m["binlog"].(map[interface{}]interface{}))
	return conf
}

func (c *Conf) initRebuildConf(m map[interface{}]interface{}) {
	rebuildConf := &RebuildConf{}
	rebuildConf.Active = getOrDefault(m, "active", false, NotCheck).(bool)
	if rebuildConf.Active {
		rebuildConf.Alias = getOrError(m, "alias", "[rebuild.alias] 不存在，请填写一个别名用来标记索引", func(v interface{}) bool { return v != "" }).(string)
		rebuildConf.Cron = getOrError(m, "cron", "[rebuild.cron] 不存在，请定义Cron表达式", func(v interface{}) bool { return v != "" }).(string)
	}
}

func (c *Conf) initBinLogConf(m map[interface{}]interface{}) {
	binLogConf := &BinLogConf{}
	binLogConf.StartBinLogName = getOrDefault(m, "startBinLogName", "", NotCheck).(string)
	binLogConf.StartBinLogPosition = getOrDefault(m, "startBinLogPosition", 0, NotCheck).(int)
	binLogConf.BinLogStatusFilePath = getOrError(m, "binLogStatusFilePath", "[binlog.binLogStatusFilePath] 不存在，请填写一个文件位置用来存储同步进度",
		func(v interface{}) bool { return v != "" }).(string)
	if !utils.IsDir(binLogConf.BinLogStatusFilePath) {
		log.Panicf("同步进度文件 %v 路径错误，请检查", binLogConf.BinLogStatusFilePath)
	}
	c.BinLogConf = binLogConf
}

func (c *Conf) initMySQLConf(m map[interface{}]interface{}) {
	mySQLConf := &MySQLConf{}
	mySQLConf.Host = getOrError(m, "host", "[mysql.host] 不存在", func(v interface{}) bool { return v != "" }).(string)
	mySQLConf.Port = getOrError(m, "port", "[mysql.port] 不存在", func(v interface{}) bool { return v != 0 }).(int)
	mySQLConf.User = getOrError(m, "user", "[mysql.user] 不存在", func(v interface{}) bool { return v != "" }).(string)
	mySQLConf.Password = getOrError(m, "password", "[mysql.password] 不存在", func(v interface{}) bool { return v != "" }).(string)
	mySQLConf.Flavor = "mysql"
	mySQLConf.UseDecimal = true
	mySQLConf.ReadTimeoutMs = getOrDefault(m, "readTimeOut", 60000, func(v interface{}) bool { return v != 0 }).(int)
	mySQLConf.HeartbeatPeriodMs = getOrDefault(m, "heartbeatPeriod", 90000, func(v interface{}) bool { return v != 0 }).(int)
	mySQLConf.ServerID = uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001
	mySQLConf.Database = getOrError(m, "database", "[mysql.database] 不存在", func(v interface{}) bool { return v != "" }).(string)
	c.MySQL = mySQLConf
}

func (c *Conf) initESConf(m map[interface{}]interface{}) {
	esConf := &ESConf{}
	esConf.Host = getOrError(m, "host", "[es.host] 不存在", func(v interface{}) bool { return v != "" }).(string)
	esConf.Port = getOrError(m, "port", "[es.port] 不存在", func(v interface{}) bool { return v != 0 }).(int)
	esConf.Index = getOrError(m, "index", "[es.index] 不存在", func(v interface{}) bool { return v != "" }).(string)
	esConf.Type = getOrError(m, "type", "[es.type] 不存在", func(v interface{}) bool { return v != "" }).(string)
	c.ES = esConf
}

func (c *Conf) initRule(m map[interface{}]interface{}) {
	rule := &Rule{}
	joinTableMap := make(map[string]*JoinTable)
	tables := getOrError(m, "tables", "[rule.tables] 不存在", func(v interface{}) bool { return v != nil }).(map[interface{}]interface{})
	for k, v := range tables {
		tableName := k.(string)
		tableInfo := v.(map[interface{}]interface{})
		collList := make(map[string]*Coll)
		mappings := getOrDefault(tableInfo, "mapping", make(map[interface{}]interface{}), func(v interface{}) bool { return v != nil }).(map[interface{}]interface{})
		for k, v := range mappings {
			collList[k.(string)] = &Coll{k.(string), 0, v.(string), fmt.Sprintf("%v.%v", tableName, k.(string))}
		}
		main := getOrDefault(tableInfo, "main", false, func(v interface{}) bool { return v != "" }).(bool)
		if main {
			if rule.MainTable != nil {
				log.Panicf("同时存在多个主表 [%v, %v]", rule.MainTable.TableName, tableName)
			}
			mainCollName := getOrError(tableInfo, "main_coll", "[rule.main_coll] 不存在，主表需要有主键字段", func(v interface{}) bool { return v != "" }).(string)
			rule.MainTable = &MainTable{tableName, collList, mainCollName}
		} else {
			joinColl := getOrError(tableInfo, "join_coll", "[rule.join_coll] 不存在，主表需要有连接主键字段", func(v interface{}) bool { return v != "" }).(string)
			joinMainColl := getOrError(tableInfo, "join_main_coll", "[rule.join_main_coll] 不存在，主表需要有连接主键字段", func(v interface{}) bool { return v != "" }).(string)
			joinTableMap[tableName] = &JoinTable{tableName, collList, joinColl, joinMainColl}
		}
	}
	if rule.MainTable == nil {
		log.Panic("不存在主表")
	}
	rule.JoinTables = joinTableMap
	c.Rule = rule
}
