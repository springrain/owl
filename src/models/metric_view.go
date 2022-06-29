package models

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
)

const MetricViewStructTableName = "metric_view"

// MetricView 在告警聚合视图查看的时候，要存储一些聚合规则
type MetricView struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id       int64  `column:"id" json:"id"`
	Name     string `column:"name" json:"name"`
	Cate     int    `column:"cate" json:"cate"` //Cate 0: preset 1: custom
	Configs  string `column:"configs" json:"configs"`
	CreateAt int64  `column:"create_at" json:"create_at"`
	CreateBy int64  `column:"create_by" json:"create_by"`
	UpdateAt int64  `column:"update_at" json:"update_at"`
	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
}

func (entity *MetricView) GetTableName() string {
	return MetricViewStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *MetricView) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (v *MetricView) Verify() error {
	v.Name = strings.TrimSpace(v.Name)
	if v.Name == "" {
		return errors.New("name is blank")
	}

	v.Configs = strings.TrimSpace(v.Configs)
	if v.Configs == "" {
		return errors.New("configs is blank")
	}

	return nil
}

func (v *MetricView) Add() error {
	if err := v.Verify(); err != nil {
		return err
	}
	now := time.Now().Unix()
	v.CreateAt = now
	v.UpdateAt = now

	return Insert(v)
}

func (v *MetricView) Update(name, configs string, cate int, createBy int64) error {
	if err := v.Verify(); err != nil {
		return err
	}
	ctx := getCtx()
	v.UpdateAt = time.Now().Unix()
	v.Name = name
	v.Configs = configs
	v.Cate = cate

	if v.CreateBy == 0 {
		v.CreateBy = createBy
	}
	// return DB().Model(v).Select("name", "configs", "cate", "update_at").Updates(v).Error
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, v)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

// MetricViewDel: userid for safe delete
func MetricViewDel(ids []int64, createBy ...interface{}) error {
	if len(ids) == 0 {
		return nil
	}
	ctx := getCtx()
	if len(createBy) > 0 {
		_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			finder := zorm.NewDeleteFinder(MetricViewStructTableName)
			finder.Append(" Where id in (?) and create_by = ?", ids, createBy[0])
			_, err := zorm.UpdateFinder(ctx, finder)
			return nil, err
		})
		return err
		// return DB().Where("id in ? and create_by = ?", ids, createBy[0]).Delete(new(MetricView)).Error
	}

	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(MetricViewStructTableName)
		finder.Append(" Where id in (?)", ids)
		_, err := zorm.UpdateFinder(ctx, finder)
		return nil, err
	})
	return err
	// return DB().Where("id in ?", ids).Delete(new(MetricView)).Error
}

func MetricViewGets(createBy interface{}) ([]MetricView, error) {
	lst := make([]MetricView, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(MetricViewStructTableName)
	finder.Append("WHERE create_by = ? or cate = 0", createBy)
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

func MetricViewGet(where string, args ...interface{}) (*MetricView, error) {
	lst := make([]*MetricView, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertRuleStructTableName)
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	_, err := zorm.QueryRow(ctx, finder, &lst)
	// err := DB().Where(where, args...).Find(&lst).Error
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}
