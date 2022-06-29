package models

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/toolkits/pkg/slice"
)

const AlertAggrViewStructTableName = "alert_aggr_view"

// AlertAggrView 在告警聚合视图查看的时候，要存储一些聚合规则
type AlertAggrView struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id       int64  `column:"id" json:"id"`
	Name     string `column:"name" json:"name"`
	Rule     string `column:"rule" json:"rule"`
	Cate     int    `column:"cate" json:"cate"` //Cate 0: preset 1: custom
	CreateAt int64  `column:"create_at" json:"create_at"`
	CreateBy int64  `column:"create_by" json:"create_by"`
	UpdateAt int64  `column:"update_at" json:"update_at"`
	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
}

//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *AlertAggrView) GetTableName() string {
	return AlertAggrViewStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *AlertAggrView) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (v *AlertAggrView) Verify() error {
	v.Name = strings.TrimSpace(v.Name)
	if v.Name == "" {
		return errors.New("name is blank")
	}

	v.Rule = strings.TrimSpace(v.Rule)
	if v.Rule == "" {
		return errors.New("rule is blank")
	}

	var validFields = []string{
		"cluster",
		"group_id",
		"group_name",
		"rule_id",
		"rule_name",
		"severity",
		"runbook_url",
		"target_ident",
		"target_note",
	}

	arr := strings.Split(v.Rule, "::")
	for i := 0; i < len(arr); i++ {
		pair := strings.Split(arr[i], ":")
		if len(pair) != 2 {
			return errors.New("rule invalid")
		}

		if !(pair[0] == "field" || pair[0] == "tagkey") {
			return errors.New("rule invalid")
		}

		if pair[0] == "field" {
			// 只支持有限的field
			if !slice.ContainsString(validFields, pair[1]) {
				return fmt.Errorf("unsupported field: %s", pair[1])
			}
		}
	}

	return nil
}

func (v *AlertAggrView) Add() error {
	if err := v.Verify(); err != nil {
		return err
	}

	now := time.Now().Unix()
	v.CreateAt = now
	v.UpdateAt = now
	v.Cate = 1

	return Insert(v)
}

func (v *AlertAggrView) Update(name, rule string, cate int, createBy int64) error {
	if err := v.Verify(); err != nil {
		return err
	}

	ctx := getCtx()
	finder := zorm.NewUpdateFinder(AlertAggrViewStructTableName)
	finder.Append("name=?,rule=?,update_at=?,cate=?", name, rule, time.Now().Unix(), cate)
	if v.CreateBy == 0 {
		// v.CreateBy = createBy
		finder.Append("create_by=?", createBy)
	}
	finder.Append(" WHERE id=?", v.Id)
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateFinder(ctx, finder)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
	// return DB().Model(v).Select("name", "rule", "update_at").Updates(v).Error
}

// AlertAggrViewDel: userid for safe delete
func AlertAggrViewDel(ids []int64, createBy ...interface{}) error {
	if len(ids) == 0 {
		return nil
	}
	ctx := getCtx()

	if len(createBy) > 0 {
		// return DB().Where("id in ? and create_by = ?", ids, createBy).Delete(new(AlertAggrView)).Error
		_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			finder := zorm.NewDeleteFinder(AlertAggrViewStructTableName)
			finder.Append(" Where id in (?) and create_by = ?", ids, createBy)
			_, err := zorm.UpdateFinder(ctx, finder)
			return nil, err
		})
		return err
	}
	// return DB().Where("id in ?", ids).Delete(new(AlertAggrView)).Error
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(AlertAggrViewStructTableName)
		finder.Append(" Where id in (?)", ids)
		_, err := zorm.UpdateFinder(ctx, finder)
		return nil, err
	})
	return err
}

func AlertAggrViewGets(createBy interface{}) ([]AlertAggrView, error) {
	lst := make([]AlertAggrView, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertAggrViewStructTableName)
	finder.Append("Where create_by = ? or cate = 0", createBy)
	err := zorm.Query(ctx, finder, &lst, nil)
	// err := DB().Where("create_by = ? or cate = 0", createBy).Find(&lst).Error
	if err == nil && len(lst) > 1 {
		sort.Slice(lst, func(i, j int) bool {
			if lst[i].Cate < lst[j].Cate {
				return true
			}

			if lst[i].Cate > lst[j].Cate {
				return false
			}

			return lst[i].Name < lst[j].Name
		})
	}
	return lst, err
}

func AlertAggrViewGet(where string, args ...interface{}) (*AlertAggrView, error) {
	lst := make([]*AlertAggrView, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertAggrViewStructTableName)
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	err := zorm.Query(ctx, finder, &lst, nil)
	// err := DB().Where(where, args...).Find(&lst).Error
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}
