package models

import (
	"gitee.com/chunanyong/zorm"
	"github.com/toolkits/pkg/slice"
)

const RoleOperationStructTableName = "role_operation"

// RoleOperation
type RoleOperation struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	RoleName  string `column:"role_name"`
	Operation string `column:"operation"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上

}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *RoleOperation) GetTableName() string {
	return RoleOperationStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *RoleOperation) GetPKColumnName() string {
	//如果没有主键
	return ""
}

func RoleHasOperation(roles []string, operation string) (bool, error) {
	if len(roles) == 0 {
		return false, nil
	}

	// return Exists(DB().Model(&RoleOperation{}).Where("operation = ? and role_name in ?", operation, roles))
	finder := zorm.NewSelectFinder(RoleOperationStructTableName)
	finder.Append("WHERE operation = ? and role_name in (?)", operation, roles)
	//查询条数
	num, err := Count(finder)
	return num > 0, err

}

func OperationsOfRole(roles []string) ([]string, error) {
	ctx := getCtx()
	// session := DB().Model(&RoleOperation{}).Select("distinct(operation) as operation")
	finder := zorm.NewSelectFinder(RoleOperationStructTableName, "distinct(operation) as operation")
	if !slice.ContainsString(roles, AdminRole) {
		// session = session.Where("role_name in ?", roles)
		finder.Append("Where role_name in (?)", roles)
	}

	ret := make([]string, 0)
	// err := session.Pluck("operation", &ret).Error
	err := zorm.Query(ctx, finder, &ret, nil)
	return ret, err
}
