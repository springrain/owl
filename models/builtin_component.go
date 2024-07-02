package models

import (
	"errors"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
)

const BuiltinComponentTableName = "builtin_components"

// BuiltinComponent represents a builtin component along with its metadata.
type BuiltinComponent struct {
	// 引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	ID        uint64 `json:"id" column:"id"`
	Ident     string `json:"ident" column:"ident"`
	Logo      string `json:"logo" column:"logo"`
	Readme    string `json:"readme" column:"readme"`
	CreatedAt int64  `json:"created_at" column:"created_at"`
	CreatedBy string `json:"created_by" column:"created_by"`
	UpdatedAt int64  `json:"updated_at" column:"updated_at"`
	UpdatedBy string `json:"updated_by" column:"updated_by"`
}

func (bc *BuiltinComponent) GetTableName() string {
	return BuiltinComponentTableName
}

func (bc *BuiltinComponent) Verify() error {
	bc.Ident = strings.TrimSpace(bc.Ident)
	if bc.Ident == "" {
		return errors.New("ident is blank")
	}

	return nil
}

func BuiltinComponentExists(ctx *ctx.Context, bc *BuiltinComponent) (bool, error) {
	finder := zorm.NewSelectFinder(BuiltinComponentTableName, "count(*)").Append("WHERE ident = ?", bc.Ident)
	count, err := Count(ctx, finder)

	//var count int64
	//err := DB(ctx).Model(bc).Where("ident = ?", bc.Ident).Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (bc *BuiltinComponent) Add(ctx *ctx.Context, username string) error {
	if err := bc.Verify(); err != nil {
		return err
	}
	exists, err := BuiltinComponentExists(ctx, bc)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("builtin component already exists")
	}
	now := time.Now().Unix()
	bc.CreatedAt = now
	bc.UpdatedAt = now
	bc.CreatedBy = username
	return Insert(ctx, bc)
}

func (bc *BuiltinComponent) Update(ctx *ctx.Context, req BuiltinComponent) error {
	if err := req.Verify(); err != nil {
		return err
	}

	if bc.Ident != req.Ident {
		exists, err := BuiltinComponentExists(ctx, &req)
		if err != nil {
			return err
		}
		if exists {
			return errors.New("builtin component already exists")
		}
	}
	req.UpdatedAt = time.Now().Unix()

	return Update(ctx, &req, nil)
	//return DB(ctx).Model(bc).Select("*").Updates(req).Error
}

func BuiltinComponentDels(ctx *ctx.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	return DeleteByIds(ctx, BuiltinComponentTableName, ids)
	//return DB(ctx).Where("id in ?", ids).Delete(new(BuiltinComponent)).Error
}

func BuiltinComponentGets(ctx *ctx.Context, query string) ([]*BuiltinComponent, error) {
	finder := zorm.NewSelectFinder(BuiltinComponentTableName).Append("WHERE 1=1")
	//session := DB(ctx)
	if query != "" {
		queryPattern := "%" + query + "%"
		//session = session.Where("ident LIKE ?", queryPattern)
		finder.Append("and ident LIKE ?", queryPattern)
	}

	//var lst []*BuiltinComponent
	lst := make([]*BuiltinComponent, 0)
	finder.Append("order by ident ASC")
	//err := session.Order("ident ASC").Find(&lst).Error
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	return lst, err
}

func BuiltinComponentGet(ctx *ctx.Context, where string, args ...interface{}) (*BuiltinComponent, error) {
	//var lst []*BuiltinComponent
	lst := make([]*BuiltinComponent, 0)
	finder := zorm.NewSelectFinder(BuiltinComponentTableName)
	AppendWhere(finder, where, args...)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Where(where, args...).Find(&lst).Error
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}
