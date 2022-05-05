package models

import (
	"context"

	"gitee.com/chunanyong/zorm"
)

//TaskRecordStructTableName 表名常量,方便直接调用
const TaskRecordStructTableName = "task_record"

// TaskRecord
type TaskRecord struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id           int64  `column:"id" json:"id"`
	GroupId      int64  `column:"group_id" json:"group_id"`
	IbexAddress  string `column:"ibex_address" json:"ibex_address"`
	IbexAuthUser string `column:"ibex_auth_user" json:"ibex_auth_user"`
	IbexAuthPass string `column:"ibex_auth_pass" json:"ibex_auth_pass"`
	Title        string `column:"title" json:"title"`
	Account      string `column:"account" json:"account"`
	Batch        int    `column:"batch" json:"batch"`
	Tolerance    int    `column:"tolerance" json:"tolerance"`
	Timeout      int    `column:"timeout" json:"timeout"`
	Pause        string `column:"pause" json:"pause"`
	Script       string `column:"script" json:"script"`
	Args         string `column:"args" json:"args"`
	CreateAt     int64  `column:"create_at" json:"create_at"`
	CreateBy     string `column:"create_by" json:"create_by"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上

}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *TaskRecord) GetTableName() string {
	return TaskRecordStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *TaskRecord) GetPKColumnName() string {
	return "id"
}

// create task
func (r *TaskRecord) Add() error {
	return Insert(r)
}

// list task, filter by group_id, create_by
func TaskRecordTotal(bgid, beginTime int64, createBy, query string) (int64, error) {

	finder := zorm.NewSelectFinder(TaskRecordStructTableName, "count(*)")
	// session := DB().Model(new(TaskRecord)).Where("create_at > ? and group_id = ?", beginTime, bgid)
	finder.Append("WHERE create_at > ? and group_id = ?", beginTime, bgid)
	if createBy != "" {
		// session = session.Where("create_by = ?", createBy)
		finder.Append("AND create_by = ?", createBy)
	}

	if query != "" {
		// session = session.Where("title like ?", "%"+query+"%")
		finder.Append("AND title like ?", "%"+query+"%")
	}

	return Count(finder)
	// return Count(session)
}

func TaskRecordGets(bgid, beginTime int64, createBy, query string, limit, offset int) ([]*TaskRecord, error) {
	ctx := getCtx()
	finder := zorm.NewSelectFinder(TaskRecordStructTableName)
	finder.Append("WHERE create_at > ? and group_id = ?", beginTime, bgid)
	if createBy != "" {
		// session = session.Where("create_by = ?", createBy)
		finder.Append("AND create_by = ?", createBy)
	}

	if query != "" {
		// session = session.Where("title like ?", "%"+query+"%")
		finder.Append("AND title like ?", "%"+query+"%")
	}

	page := zorm.NewPage()
	page.PageNo = offset/limit + 1 //查询第1页,默认是1
	page.PageSize = limit
	finder.Append("Order by create_at desc")
	// session := DB().Where("create_at > ? and group_id = ?", beginTime, bgid).Order("create_at desc").Limit(limit).Offset(offset)

	lst := make([]*TaskRecord, 0)
	// err := session.Find(&lst).Error
	err := zorm.Query(ctx, finder, &lst, page)
	return lst, err
}

// update is_done fiel
func (r *TaskRecord) UpdateIsDone(isDone int) error {
	// return DB().Model(r).Update("is_done", isDone).Error
	ctx := getCtx()
	finder := zorm.NewUpdateFinder(TaskRecordStructTableName).Append("is_done=? WHERE id=?", isDone, r.Id)
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {

		_, err := zorm.UpdateFinder(ctx, finder)
		return nil, err
	})
	return err

}
