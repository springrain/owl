package models

import (
	"context"

	"gitee.com/chunanyong/zorm"
)

//ChartStructTableName 表名常量,方便直接调用
const ChartStructTableName = "chart"

// ChartStruct
type Chart struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id      int64  `column:"id" json:"id"`
	GroupId int64  `column:"group_id" json:"group_id"`
	Configs string `column:"configs" json:"configs"`
	Weight  int    `column:"weight" json:"weight"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Chart) GetTableName() string {
	return ChartStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Chart) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func ChartsOf(chartGroupId int64) ([]Chart, error) {
	objs := make([]Chart, 0)
	// err := DB().Where("group_id = ?", chartGroupId).Order("weight").Find(&objs).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(ChartStructTableName) // select * from t_demo

	finder.Append("Where group_id = ?", chartGroupId).Append("Order by weight")
	//执行查询
	err := zorm.Query(ctx, finder, &objs, nil)
	return objs, err
}

func (c *Chart) Add() error {
	return Insert(c)

}

func (c *Chart) Update(selectField interface{}, selectFields ...interface{}) error {
	// return DB().Model(c).Select(selectField, selectFields...).Updates(c).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {

		_, err := zorm.UpdateNotZeroValue(ctx, c)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (c *Chart) Del() error {
	// return DB().Where("id=?", c.Id).Delete(&Chart{}).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(ChartStructTableName)
		finder.Append("Where id=?", c.Id)
		_, err := zorm.UpdateFinder(ctx, finder)

		return nil, err
	})
	return err
}
