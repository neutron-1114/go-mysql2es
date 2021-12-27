package main

import (
	"flag"
	log "github.com/sirupsen/logrus"
	"go-mysql2es/src/config"
	"go-mysql2es/src/core"
)

type Context struct {
	conf *config.Conf
}

func main() {
	var confPath = flag.String("conf", "", "配置文件路径")
	flag.Parse()
	if *confPath == "" {
		log.Panic("-conf 配置文件路径")
	}
	conf := config.Load(confPath)
	syncer := core.New(conf)
	syncer.Prepare()
	syncer.Run()
}
