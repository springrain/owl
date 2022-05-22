package ormx

import (

	// "time"
	//_"gorm.io/driver/mysql"
	// "gorm.io/driver/postgres"
	// "gorm.io/gorm"
	// "gorm.io/gorm/schema"
	"gitee.com/chunanyong/zorm"
	// _ "kingbase.com/gokb"
	_ "github.com/go-sql-driver/mysql"
)

// DBConfig ZORM DBConfig
type DBConfig struct {
	Debug        bool
	DBType       string
	DSN          string
	DriverName   string
	MaxLifetime  int
	MaxOpenConns int
	MaxIdleConns int
	TablePrefix  string
}

//var DB *zorm.DBDao

// New Create zorm.DBDao
func New(c DBConfig) (*zorm.DBDao, error) {

	dbDaoConfig := zorm.DataSourceConfig{
		//DSN 数据库的连接字符串
		DSN: c.DSN,
		//数据库驱动名称:mysql,postgres,oci8,sqlserver,sqlite3,dm,kingbase 和DBType对应,处理数据库有多个驱动
		DriverName: c.DriverName,
		//数据库类型(方言判断依据):mysql,postgresql,oracle,mssql,sqlite,dm,kingbase 和 DriverName 对应,处理数据库有多个驱动
		DBType: c.DBType,
		//MaxOpenConns 数据库最大连接数 默认50
		MaxOpenConns: c.MaxOpenConns,
		//MaxIdleConns 数据库最大空闲连接数 默认50
		MaxIdleConns: c.MaxIdleConns,
		//ConnMaxLifetimeSecond 连接存活秒时间. 默认600(10分钟)后连接被销毁重建.避免数据库主动断开连接,造成死连接.MySQL默认wait_timeout 28800秒(8小时)
		ConnMaxLifetimeSecond: c.MaxLifetime,
		//PrintSQL 打印SQL.会使用FuncPrintSQL记录SQL
		PrintSQL: c.Debug,
	}

	// 根据dbDaoConfig创建dbDao, 一个数据库只执行一次,第一个执行的数据库为 defaultDao,后续zorm.xxx方法,默认使用的就是defaultDao
	dbDao, err := zorm.NewDBDao(&dbDaoConfig)
	//DB = dbDao
	return dbDao, err
}
