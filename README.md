# go-mysql2es
###没有进行大规模测试，仅仅是学习使用，切勿在线上使用，或者修改后再使用
自用Mysql同步Elasticsearch工具

go build -o ./bin/go-mysql2es ./src/main.go

支持主表与多个附表连表，形成宽表
但只能是一主对多附，不能附对附

使用要求：
1. 联表对应的主表字段必需有索引，并且必需配置被同步到ES
2. 主表的主键ID是数字类型

//TODO
1、增加binlog消费能力，按id多线程hash，保证同一id下的数据有序