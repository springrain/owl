package models

import (
	"context"

	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/str"
)

//ChartGroupStructTableName 表名常量,方便直接调用
const ChartGroupStructTableName = "chart_group"

// ChartGroupStruct
type ChartGroup struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id          int64  `column:"id" json:"id"`
	DashboardId int64  `column:"dashboard_id" json:"dashboard_id"`
	Name        string `column:"name" json:"name"`
	Weight      int    `column:"weight" json:"weight"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上

}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *ChartGroup) GetTableName() string {
	return ChartGroupStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *ChartGroup) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (cg *ChartGroup) Verify() error {
	if cg.DashboardId <= 0 {
		return errors.New("Arg(dashboard_id) invalid")
	}

	if str.Dangerous(cg.Name) {
		return errors.New("Name has invalid characters")
	}

	return nil
}

func (cg *ChartGroup) Add() error {
	if err := cg.Verify(); err != nil {
		return err
	}
	return Insert(cg)
}

func (cg *ChartGroup) Update(selectField interface{}, selectFields ...interface{}) error {
	if err := cg.Verify(); err != nil {
		return err
	}

	// return DB().Model(cg).Select(selectField, selectFields...).Updates(cg).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {

		_, err := zorm.UpdateNotZeroValue(ctx, cg)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (cg *ChartGroup) Del() error {
	// return DB().Transaction(func(tx *gorm.DB) error {
	// 	if err := tx.Where("group_id=?", cg.Id).Delete(&Chart{}).Error; err != nil {
	// 		return err
	// 	}

	// 	if err := tx.Where("id=?", cg.Id).Delete(&ChartGroup{}).Error; err != nil {
	// 		return err
	// 	}

	// 	return nil
	// })

	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(ChartStructTableName)
		finder.Append("Where group_id=?", cg.Id)
		_, err := zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}

		finder = zorm.NewDeleteFinder(ChartGroupStructTableName)
		finder.Append("Where id=?", cg.Id)
		_, err = zorm.UpdateFinder(ctx, finder)

		return nil, err
	})
	return err

}

func NewDefaultChartGroup(dashId int64) error {
	return Insert(&ChartGroup{
		DashboardId: dashId,
		Name:        "Default chart group",
		Weight:      0,
	})

}

func ChartGroupIdsOf(dashId int64) ([]int64, error) {
	ids := make([]int64, 0)
	// err := DB().Model(&ChartGroup{}).Where("dashboard_id = ?", dashId).Pluck("id", &ids).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(ChartGroupStructTableName, "id")
	finder.Append("Where dashboard_id = ?", dashId)
	err := zorm.Query(ctx, finder, &ids, nil)
	return ids, err
}

func ChartGroupsOf(dashId int64) ([]ChartGroup, error) {
	objs := make([]ChartGroup, 0)
	// err := DB().Where("dashboard_id = ?", dashId).Order("weight").Find(&objs).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(ChartGroupStructTableName) // select * from t_demo

	finder.Append("Where dashboard_id = ?", dashId).Append("Order by weight")
	//执行查询
	err := zorm.Query(ctx, finder, &objs, nil)
	return objs, err
}
