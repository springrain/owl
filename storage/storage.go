package storage

import (
	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ormx"
)

var DB *zorm.DBDao

func New(cfg ormx.DBConfig) (*zorm.DBDao, error) {
	db, err := ormx.New(cfg)
	if err != nil {
		return nil, err
	}

	return db, nil
}
