package core

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"go-mysql2es/src/config"
	"go-mysql2es/src/db"
	"go-mysql2es/src/es"
	"go-mysql2es/src/handler"
	"go-mysql2es/src/utils"
	"os"
	"strings"
	"time"
)

type Syncer struct {
	Conf        *config.Conf
	EsClient    *es.Client
	MysqlClient *db.DB
}

func New(conf *config.Conf) *Syncer {
	return &Syncer{conf, es.New(conf), db.New(conf)}
}

func (syncer *Syncer) Prepare() {
	// 使用 MYSQL 表信息补全列类型
	syncer.MysqlClient.FillCollType()
}

func (syncer *Syncer) Run() {
	count, err := syncer.EsClient.Count()
	if err != nil {
		log.Panicf("获取索引状态失败, %v", err)
	}
	position := &mysql.Position{
		Name: "",
		Pos:  0,
	}
	hashKey := fmt.Sprintf("%v%v%v%v", syncer.Conf.ES.Host, syncer.Conf.ES.Port, syncer.Conf.ES.Index, syncer.Conf.ES.Type)
	statusFilePath := fmt.Sprintf("%v/%v.status", syncer.Conf.BinLogConf.BinLogStatusFilePath, utils.GetHashFromStr(hashKey))
	if count == 0 {
		syncer.full()
		if syncer.Conf.BinLogConf.StartBinLogName == "" {
			position.Name = syncer.MysqlClient.GetOldestBinlogName()
			position.Pos = 0
		} else {
			position.Name = syncer.Conf.BinLogConf.StartBinLogName
			position.Pos = uint32(syncer.Conf.BinLogConf.StartBinLogPosition)
		}

	} else {
		if utils.IsFile(statusFilePath) {
			lines, err := utils.File2list(statusFilePath, utils.CommonHandler)
			if err != nil {
				log.Panicf("读取状态文件错误, %v", err)
			}
			info := strings.Split(lines[0], "\t")
			position.Name = info[0]
			position.Pos = uint32(utils.Str2Int(info[1]))
		} else {
			if syncer.Conf.BinLogConf.StartBinLogName == "" {
				position.Name = syncer.MysqlClient.GetOldestBinlogName()
				position.Pos = 0
			} else {
				position.Name = syncer.Conf.BinLogConf.StartBinLogName
				position.Pos = uint32(syncer.Conf.BinLogConf.StartBinLogPosition)
			}
		}
	}
	syncer.incr(position, statusFilePath)
}

func (syncer *Syncer) incr(position *mysql.Position, statusFilePath string) {
	log.Infof("[INCR] 准备开始同步..., %v %v", position.Name, position.Pos)
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%v:%v", syncer.Conf.MySQL.Host, syncer.Conf.MySQL.Port)
	cfg.User = syncer.Conf.MySQL.User
	cfg.Password = syncer.Conf.MySQL.Password
	cfg.Flavor = syncer.Conf.MySQL.Flavor
	cfg.UseDecimal = syncer.Conf.MySQL.UseDecimal
	cfg.ReadTimeout = time.Duration(syncer.Conf.MySQL.ReadTimeoutMs) * time.Millisecond
	cfg.HeartbeatPeriod = time.Duration(syncer.Conf.MySQL.HeartbeatPeriodMs) * time.Millisecond
	cfg.ServerID = syncer.Conf.MySQL.ServerID
	cfg.Dump.ExecutionPath = ""
	var syncTables []string
	//只同步需要的表
	syncTables = append(syncTables, fmt.Sprintf("%v.%v", syncer.Conf.MySQL.Database, syncer.Conf.Rule.MainTable.TableName))
	for k := range syncer.Conf.Rule.JoinTables {
		syncTables = append(syncTables, fmt.Sprintf("%v.%v", syncer.Conf.MySQL.Database, k))
	}
	cfg.IncludeTableRegex = syncTables
	c, err := canal.NewCanal(cfg)
	if err != nil {
		log.Panicf("[INCR] 建立 canal 失败, %v", err)
	}
	h := handler.New(syncer.Conf.Rule, syncer.EsClient, syncer.MysqlClient)
	c.SetEventHandler(h)
	go func() {
		err = c.RunFrom(*position)
		if err != nil {
			log.Panicf("[INCR] 启动 canal 失败, %v", err)
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second) // 每隔1s进行一次打印
		for {
			<-ticker.C
			p := c.SyncedPosition()
			mp, _ := c.GetMasterPos()
			log.Infof("[INCR] P: %v -> %v MP: %v -> %v delay: %v ...", p.Name, p.Pos, mp.Name, mp.Pos, c.GetDelay())
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second * 60) // 每隔1s进行一次打印
		for {
			<-ticker.C
			stat := h.RefreshAndGetStat()
			var lines []string
			for tableName, st := range stat {
				lines = append(lines, fmt.Sprintf("[%v -> (i:%v,u:%v,d:%v)]", tableName, st["i"], st["u"], st["d"]))
			}
			log.Infof("[INCR] stat: %v", strings.Join(lines, " "))
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second) // 每隔1s将当前状态写入到状态文件
		for {
			<-ticker.C
			f, err := os.OpenFile(statusFilePath, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
			if err != nil {
				log.Error(err)
			} else {
				p := c.SyncedPosition()
				line := fmt.Sprintf("%v\t%v", p.Name, p.Pos)
				n, _ := f.Seek(0, os.SEEK_END)
				_, err = f.WriteAt([]byte(line), n)
				_ = f.Close()
			}
		}
	}()
	<-make(chan bool)
}

func (syncer *Syncer) full() {
	minId, maxId := syncer.MysqlClient.GetIdRange()
	var count = 0
	var startTime = time.Now().Unix()
	start := minId
	for start <= maxId {
		resultList := syncer.MysqlClient.FullGetByRange(start, start+1000)
		start += 1000
		err := syncer.EsClient.BatchInsert(resultList)
		if err != nil {
			log.Error(err)
		}
		count += len(resultList)
		cost := time.Now().Unix() - startTime
		log.Infof("[FULL] execute: %v cost: %v avg: %v", count, cost, float64(cost)/float64(count))
	}
	log.Infof("[FULL] finished!!! cost: %v", time.Now().Unix()-startTime)
}
