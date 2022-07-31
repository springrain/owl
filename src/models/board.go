package models

import (
	"strings"
	"time"
	
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/str"
	"gitee.com/chunanyong/zorm"
	"context"
)

//BoardStructTableName 表名常量,方便直接调用
const BoardStructTableName = "board"

type Board struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	//Id []
	Id       int64  `column:"id" json:"id"`
	GroupId  int64  `column:"group_id" json:"group_id"`
	Name     string `column:"name" json:"name"`
	Tags     string `column:"tags" json:"tags"`
	CreateAt int64  `column:"create_at" json:"create_at"`
	CreateBy string `column:"create_by" json:"create_by"`
	UpdateAt int64  `column:"update_at" json:"update_at"`
	UpdateBy string `column:"update_by" json:"update_by"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	Configs  string `json:"configs"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Board) GetTableName() string {
	return BoardStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Board) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (b *Board) Verify() error {
	if b.Name == "" {
		return errors.New("Name is blank")
	}

	if str.Dangerous(b.Name) {
		return errors.New("Name has invalid characters")
	}

	return nil
}

func (b *Board) Add() error {
	if err := b.Verify(); err != nil {
		return err
	}

	now := time.Now().Unix()
	b.CreateAt = now
	b.UpdateAt = now

	return Insert(b)
}

func (b *Board) Update(selectField interface{}, selectFields ...interface{}) error {
	if err := b.Verify(); err != nil {
		return err
	}

	// return DB().Model(b).Select(selectField, selectFields...).Updates(b).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, b)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (b *Board) Del() error {
	// return DB().Transaction(func(tx *gorm.DB) error {
	// 	if err := tx.Where("id=?", b.Id).Delete(&BoardPayload{}).Error; err != nil {
	// 		return err
	// 	}

	// 	if err := tx.Where("id=?", b.Id).Delete(&Board{}).Error; err != nil {
	// 		return err
	// 	}

	// 	return nil
	// })
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(BoardPayloadStructTableName)
		finder.Append("Where id=?", b.Id)
		_, err := zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}

		finder = zorm.NewDeleteFinder(BoardStructTableName)
		finder.Append("Where id=?", b.Id)
		_, err = zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}
		return nil, err
	})
	return err
}

func BoardGetByID(id int64) (*Board, error) {
	var lst []*Board
	ctx := getCtx()
	// err := DB().Where("id = ?", id).Find(&lst).Error
	finder := zorm.NewSelectFinder(BoardStructTableName)
	finder.Append("Where id=?", id)
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}

// BoardGet for detail page
func BoardGet(where string, args ...interface{}) (*Board, error) {
	// var lst []*Board
	// err := DB().Where(where, args...).Find(&lst).Error
	lst := make([]*Board, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(BoardStructTableName) // select * from t_demo
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	payload, err := BoardPayloadGet(lst[0].Id)
	if err != nil {
		return nil, err
	}

	lst[0].Configs = payload

	return lst[0], nil
}

func BoardCount(where string, args ...interface{}) (num int64, err error) {
	finder := zorm.NewSelectFinder(BoardStructTableName, "count(*)")
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	return Count(finder)
	// return Count(DB().Model(&Board{}).Where(where, args...))
}

func BoardExists(where string, args ...interface{}) (bool, error) {
	num, err := BoardCount(where, args...)
	return num > 0, err
}

// BoardGets for list page
func BoardGets(groupId int64, query string) ([]Board, error) {
	// session := DB().Where("group_id=?", groupId).Order("name")
	ctx := getCtx()
	finder := zorm.NewSelectFinder(BoardStructTableName) // select * from t_demo
	finder.Append("Where group_id=?", groupId)
	arr := strings.Fields(query)
	if len(arr) > 0 {
		for i := 0; i < len(arr); i++ {
			if strings.HasPrefix(arr[i], "-") {
				q := "%" + arr[i][1:] + "%"
				// session = session.Where("name not like ? and tags not like ?", q, q)
				finder.Append(" And name not like ? and tags not like ?", q, q)
			} else {
				q := "%" + arr[i] + "%"
				// session = session.Where("(name like ? or tags like ?)", q, q)
				finder.Append(" And (name like ? or tags like ?)", q, q)
			}
		}
	}

	// var objs []Board
	// err := session.Find(&objs).Error
	objs := make([]Board, 0)
	finder.Append(" Order by name")
	err := zorm.Query(ctx, finder, &objs, nil)
	return objs, err
}
