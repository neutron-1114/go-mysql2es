package handler

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	log "github.com/sirupsen/logrus"
	"go-mysql2es/src/config"
	"go-mysql2es/src/db"
	"go-mysql2es/src/es"
)

type EsSyncHandler struct {
	rule        *config.Rule
	EsClient    *es.Client
	MysqlClient *db.DB
	Stat        map[string]map[string]int64
}

func New(rule *config.Rule, es *es.Client, db *db.DB) *EsSyncHandler {
	h := &EsSyncHandler{rule, es, db, nil}
	h.RefreshAndGetStat()
	return h
}

func (h *EsSyncHandler) RefreshAndGetStat() map[string]map[string]int64 {
	old := h.Stat
	stat := make(map[string]map[string]int64)
	stat[h.rule.MainTable.TableName] = map[string]int64{
		"i": 0,
		"u": 0,
		"d": 0,
	}
	for _, t := range h.rule.JoinTables {
		stat[t.TableName] = map[string]int64{
			"i": 0,
			"u": 0,
			"d": 0,
		}
	}
	h.Stat = stat
	return old
}

func (h *EsSyncHandler) rowsEventHandler(e *canal.RowsEvent) (affected int64, err error) {
	eventAction := e.Action
	d, _ := json.Marshal(e.Rows)
	fmt.Println(fmt.Sprintf("[%v] %v", eventAction, string(d)))
	switch eventAction {
	case canal.DeleteAction:
		return h.deleteEventHandler(e)
	case canal.UpdateAction:
		return h.updateEventHandler(e)
	case canal.InsertAction:
		return h.insertEventHandler(e)
	default:
		return 0, nil
	}
}

func (h *EsSyncHandler) insertEventHandler(e *canal.RowsEvent) (affected int64, err error) {
	//主表插入：请求MYSQL查询所有结果 -> ES.UPSERT
	//副表插入：拿到所有符合的数据重新查询
	if e.Table.Name == h.rule.MainTable.TableName {
		h.Stat[h.rule.MainTable.TableName]["i"]++
		mainIndex := -1
		for i, coll := range e.Table.Columns {
			if coll.Name == h.rule.MainTable.MainCollName {
				mainIndex = i
				break
			}
		}
		if mainIndex == -1 {
			return 0, err
		}
		for _, row := range e.Rows {
			id := row[mainIndex]
			resultList := h.MysqlClient.FullGetById([]interface{}{id})
			err := h.EsClient.BatchInsert(resultList)
			if err != nil {
				log.Error(err)
			}
			affected++
		}
	} else {
		mainIndex := -1
		joinTable := h.rule.JoinTables[e.Table.Name]
		if joinTable == nil {
			return 0, nil
		}
		h.Stat[joinTable.TableName]["i"]++
		for i, coll := range e.Table.Columns {
			if coll.Name == joinTable.JoinCollName {
				mainIndex = i
				break
			}
		}
		if mainIndex == -1 {
			return 0, err
		}
		ids := map[interface{}]bool{}
		//update返回修改前与修改后两条数据，有可能并没有涉及ID的变化，所以ID相同的时候只需修改一条
		for _, row := range e.Rows {
			ids[row[mainIndex]] = true
		}
		for id := range ids {
			mainIds := h.MysqlClient.GetMainIdsByJoinId(id, joinTable.TableName)
			resultList := h.MysqlClient.FullGetById(mainIds)
			for _, result := range resultList {
				err := h.EsClient.Upsert(result)
				if err != nil {
					log.Error(err)
				}
			}
			affected++
		}
	}
	return affected, err
}

func (h *EsSyncHandler) deleteEventHandler(e *canal.RowsEvent) (affected int64, err error) {
	//主表删除：删除索引记录
	//副表插入：拿到所有符合的数据重新查询
	if e.Table.Name == h.rule.MainTable.TableName {
		h.Stat[h.rule.MainTable.TableName]["d"]++
		mainIndex := -1
		for i, coll := range e.Table.Columns {
			if coll.Name == h.rule.MainTable.MainCollName {
				mainIndex = i
				break
			}
		}
		if mainIndex == -1 {
			return 0, err
		}
		for _, row := range e.Rows {
			id := row[mainIndex]
			err := h.EsClient.Delete(id)
			if err != nil {
				log.Error(err)
			}
			affected++
		}
	} else {
		mainIndex := -1
		joinTable := h.rule.JoinTables[e.Table.Name]
		if joinTable == nil {
			return 0, nil
		}
		h.Stat[joinTable.TableName]["d"]++
		for i, coll := range e.Table.Columns {
			if coll.Name == joinTable.JoinCollName {
				mainIndex = i
				break
			}
		}
		if mainIndex == -1 {
			return 0, err
		}
		ids := map[interface{}]bool{}
		//update返回修改前与修改后两条数据，有可能并没有涉及ID的变化，所以ID相同的时候只需修改一条
		for _, row := range e.Rows {
			ids[row[mainIndex]] = true
		}
		for id := range ids {
			mainIds := h.MysqlClient.GetMainIdsByJoinId(id, joinTable.TableName)
			resultList := h.MysqlClient.FullGetById(mainIds)
			for _, result := range resultList {
				err := h.EsClient.Upsert(result)
				if err != nil {
					log.Error(err)
				}
			}
			affected++
		}
	}
	return affected, err
}

func (h *EsSyncHandler) updateEventHandler(e *canal.RowsEvent) (affected int64, err error) {
	//主表更新：查询MYSQL全量数据插入
	//副表插入：拿到所有符合的数据重新查询
	if e.Table.Name == h.rule.MainTable.TableName {
		h.Stat[h.rule.MainTable.TableName]["u"]++
		mainIndex := -1
		for i, coll := range e.Table.Columns {
			if coll.Name == h.rule.MainTable.MainCollName {
				mainIndex = i
				break
			}
		}
		if mainIndex == -1 {
			return 0, err
		}
		for _, row := range e.Rows {
			id := row[mainIndex]
			resultList := h.MysqlClient.FullGetById([]interface{}{id})
			for _, result := range resultList {
				err := h.EsClient.Upsert(result)
				if err != nil {
					log.Error(err)
				}
			}
			affected++
		}
	} else {
		mainIndex := -1
		joinTable := h.rule.JoinTables[e.Table.Name]
		if joinTable == nil {
			return 0, nil
		}
		h.Stat[joinTable.TableName]["u"]++
		for i, coll := range e.Table.Columns {
			if coll.Name == joinTable.JoinCollName {
				mainIndex = i
				break
			}
		}
		if mainIndex == -1 {
			return 0, err
		}
		ids := map[interface{}]bool{}
		//update返回修改前与修改后两条数据，有可能并没有涉及ID的变化，所以ID相同的时候只需修改一条
		for _, row := range e.Rows {
			ids[row[mainIndex]] = true
		}
		for id := range ids {
			mainIds := h.MysqlClient.GetMainIdsByJoinId(id, joinTable.TableName)
			resultList := h.MysqlClient.FullGetById(mainIds)
			for _, result := range resultList {
				err := h.EsClient.Upsert(result)
				if err != nil {
					log.Error(err)
				}
			}
			affected++
		}
	}
	return affected, err
}

func (h *EsSyncHandler) OnRotate(*replication.RotateEvent) error          { return nil }
func (h *EsSyncHandler) OnTableChanged(schema string, table string) error { return nil }
func (h *EsSyncHandler) OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}
func (h *EsSyncHandler) OnRow(e *canal.RowsEvent) error {
	_, err := h.rowsEventHandler(e)
	if err != nil {
		log.Error(err)
	}
	return nil
}
func (h *EsSyncHandler) OnXID(mysql.Position) error                            { return nil }
func (h *EsSyncHandler) OnGTID(mysql.GTIDSet) error                            { return nil }
func (h *EsSyncHandler) OnPosSynced(mysql.Position, mysql.GTIDSet, bool) error { return nil }

func (h *EsSyncHandler) String() string { return "RevertEventHandler" }
