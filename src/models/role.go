package models

import (
	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
)

const RoleStructTableName = "role"

// Role
type Role struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id   int64  `column:"id" json:"id"`
	Name string `column:"name" json:"name"`
	Note string `column:"note" json:"note"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上

}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Role) GetTableName() string {
	return RoleStructTableName
}

func (entity *Role) GetPKColumnName() string {
	return "id"
}

func RoleGets(where string, args ...interface{}) ([]Role, error) {
	objs := make([]Role, 0)
	// err := DB().Where(where, args...).Order("name").Find(&objs).Error

	ctx := getCtx()
	finder := zorm.NewSelectFinder(RoleStructTableName) // select * from t_demo
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	err := zorm.Query(ctx, finder, &objs, nil)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to query roles")
	}
	return objs, nil
}

func RoleGetsAll() ([]Role, error) {
	return RoleGets("")
}
