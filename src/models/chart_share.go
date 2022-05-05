package models

import (
	"gitee.com/chunanyong/zorm"
)

//ChartShareStructTableName 表名常量,方便直接调用
const ChartShareStructTableName = "chart_share"

// ChartShare
type ChartShare struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id       int64  `column:"id" json:"id"`
	Cluster  string `column:"cluster" json:"cluster"`
	Configs  string `column:"configs" json:"configs"`
	CreateAt int64  `column:"create_at" json:"create_at"`
	CreateBy string `column:"create_by" json:"create_by"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上

}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *ChartShare) GetTableName() string {
	return ChartShareStructTableName
}

func (entity *ChartShare) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (cs *ChartShare) Add() error {
	return Insert(cs)
}

func ChartShareGetsByIds(ids []int64) ([]ChartShare, error) {
	lst := make([]ChartShare, 0)
	if len(ids) == 0 {
		return lst, nil
	}

	// err := DB().Where("id in ?", ids).Order("id").Find(&lst).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(ChartShareStructTableName) // select * from t_demo

	finder.Append("Where id in (?)", ids).Append("Order by id")
	//执行查询
	err := zorm.Query(ctx, finder, &lst, nil)
	return lst, err
}
