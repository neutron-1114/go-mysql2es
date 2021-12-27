package main

import (
	"flag"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	log "github.com/sirupsen/logrus"
	"go-mysql2es/src/config"
	"go-mysql2es/src/core"
	"go-mysql2es/src/utils"
	"time"
)

type Context struct {
	conf *config.Conf
}

//切割日志和清理过期日志
func configLocalFilesystemLogger(filePath string) {
	writer, err := rotatelogs.New(
		filePath+".%Y%m%d",
		rotatelogs.WithLinkName(filePath),         // 生成软链，指向最新日志文件
		rotatelogs.WithRotationTime(time.Hour*12), // 日志切割时间间隔
		rotatelogs.WithRotationCount(3),
	)
	if err != nil {
		log.Panicf("Init log failed, err:", err)
	}
	log.SetOutput(writer)
	log.SetLevel(log.InfoLevel)
}

func main() {
	var confPath = flag.String("conf", "", "配置文件路径")
	var logPath = flag.String("log", "", "日志路径")
	flag.Parse()
	if *confPath == "" {
		log.Panic("-conf 配置文件路径")
	}
	if *logPath == "" {
		configLocalFilesystemLogger("./mysql2es.log")
	} else {
		if !utils.IsDir(*logPath) {
			log.Panicf("日志文件路径 %v 检测失败", *logPath)
		} else {
			configLocalFilesystemLogger(*logPath + "/mysql2es.log")
		}
	}
	conf := config.Load(confPath)
	syncer := core.New(conf)
	syncer.Prepare()
	syncer.Run()
}
