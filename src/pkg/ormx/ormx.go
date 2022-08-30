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
	Dialect      string
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
		//DriverName 数据库驱动名称:mysql,postgres,oci8,sqlserver,sqlite3,go_ibm_db,clickhouse,dm,kingbase,aci,taosSql|taosRestful 和Dialect对应
		DriverName: c.DriverName,
		//Dialect 数据库方言:mysql,postgresql,oracle,mssql,sqlite,db2,clickhouse,dm,kingbase,shentong,tdengine 和 DriverName 对应
		Dialect: c.Dialect,
		//MaxOpenConns 数据库最大连接数 默认50
		MaxOpenConns: c.MaxOpenConns,
		//MaxIdleConns 数据库最大空闲连接数 默认50
		MaxIdleConns: c.MaxIdleConns,
		//ConnMaxLifetimeSecond 连接存活秒时间. 默认600(10分钟)后连接被销毁重建.避免数据库主动断开连接,造成死连接.MySQL默认wait_timeout 28800秒(8小时)
		ConnMaxLifetimeSecond: c.MaxLifetime,
	}
	if !c.Debug {
		dbDaoConfig.SlowSQLMillis = -1
	}

	// 根据dbDaoConfig创建dbDao, 一个数据库只执行一次,第一个执行的数据库为 defaultDao,后续zorm.xxx方法,默认使用的就是defaultDao
	dbDao, err := zorm.NewDBDao(&dbDaoConfig)
	//DB = dbDao
	return dbDao, err
}
