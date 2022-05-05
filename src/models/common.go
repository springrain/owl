package models

import (
	"gitee.com/chunanyong/zorm"
	"github.com/toolkits/pkg/str"

	"context"

	"github.com/didi/nightingale/v5/src/storage"
)

const AdminRole = "Admin"

func getCtx() context.Context {
	//ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	//defer cancel()
	var ctx = context.Background()

	ctx, err := storage.DB.BindContextDBConnection(ctx)
	if err != nil {
		panic("BindContextDBConnection err")
	}
	return ctx
}

/**
func DB() *gorm.DB {
	return storage.DB
}

func Count(tx *gorm.DB) (int64, error) {
	var cnt int64
	err := tx.Count(&cnt).Error
	return cnt, err
}
**/
func Count(fidner *zorm.Finder) (int64, error) {
	ctx := getCtx()
	var cnt int64
	_, err := zorm.QueryRow(ctx, fidner, &cnt)
	return cnt, err
}

func Insert(v zorm.IEntityStruct) error {
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.Insert(ctx, v)
		return nil, err
	})
	return err
	//return DB().Create(obj).Error
}

// CryptoPass crypto password use salt
func CryptoPass(raw string) (string, error) {
	salt, err := ConfigsGet("salt")
	if err != nil {
		return "", err
	}

	return str.MD5(salt + "<-*Uk30^96eY*->" + raw), nil
}

type Statistics struct {
	Total       int64 `column:"total"`
	LastUpdated int64 `column:"last_updated"`
}
