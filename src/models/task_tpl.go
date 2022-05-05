package models

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"context"

	"gitee.com/chunanyong/zorm"
	"github.com/toolkits/pkg/str"
)

const TaskTplStructTableName = "task_tpl"

// TaskTpl
type TaskTpl struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id        int    `column:"id" json:"id"`
	GroupId   int64  `column:"group_id" json:"group_id"`
	Title     string `column:"title" json:"title"`
	Account   string `column:"account" json:"account"`
	Batch     int    `column:"batch" json:"batch"`
	Tolerance int    `column:"tolerance" json:"tolerance"`
	Timeout   int    `column:"timeout" json:"timeout"`
	Pause     string `column:"pause" json:"pause"`
	Script    string `column:"script" json:"script"`
	Args      string `column:"args" json:"args"`
	Tags      string `column:"tags" json:"-"`
	CreateAt  int64  `column:"create_at" json:"create_at"`
	CreateBy  string `column:"create_by" json:"create_by"`
	UpdateAt  int64  `column:"update_at" json:"update_at"`
	UpdateBy  string `column:"update_by" json:"update_by"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	TagsJSON []string `json:"tags"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *TaskTpl) GetTableName() string {
	return TaskTplStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *TaskTpl) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func TaskTplTotal(groupId int64, query string) (int64, error) {
	finder := zorm.NewSelectFinder(TaskTplStructTableName, "count(*)")
	finder.Append("Where group_id = ?", groupId)

	// session := DB().Model(&TaskTpl{}).Where("group_id = ?", groupId)
	if query == "" {
		return Count(finder)
	}

	arr := strings.Fields(query)
	for i := 0; i < len(arr); i++ {
		arg := "%" + arr[i] + "%"
		// session = session.Where("title like ? or tags like ?", arg, arg)
		finder.Append("AND title like ? or tags like ?", arg, arg)
	}

	return Count(finder)
}

func TaskTplGets(groupId int64, query string, limit, offset int) ([]TaskTpl, error) {
	// session := DB().Where("group_id = ?", groupId).Order("title").Limit(limit).Offset(offset)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(TaskTplStructTableName)
	finder.Append("Where group_id = ?", groupId)
	page := zorm.NewPage()
	if offset == 0 {
		page.PageNo = offset + 1 //查询第1页,默认是1
	} else {
		page.PageNo = offset/limit + 1 //查询第1页,默认是1
	}
	tpls := make([]TaskTpl, 0)
	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			arg := "%" + arr[i] + "%"
			// session = session.Where("title like ? or tags like ?", arg, arg)
			finder.Append("AND title like ? or tags like ?", arg, arg)
		}
	}
	finder.Append("Order by title")
	// err := session.Find(&tpls).Error
	err := zorm.Query(ctx, finder, &tpls, page)
	if err == nil {
		for i := 0; i < len(tpls); i++ {
			tpls[i].TagsJSON = strings.Fields(tpls[i].Tags)
		}
	}

	return tpls, err
}

func TaskTplGet(where string, args ...interface{}) (*TaskTpl, error) {
	arr := make([]*TaskTpl, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(TaskTplStructTableName)
	finder.Append("Where "+where, args...)
	err := zorm.Query(ctx, finder, &arr, nil)
	// err := DB().Where(where, args...).Find(&arr).Error
	if err != nil {
		return nil, err
	}

	if len(arr) == 0 {
		return nil, nil
	}

	arr[0].TagsJSON = strings.Fields(arr[0].Tags)

	return arr[0], nil
}

func (t *TaskTpl) CleanFields() error {
	if t.Batch < 0 {
		return errors.New("arg(batch) should be nonnegative")
	}

	if t.Tolerance < 0 {
		return errors.New("arg(tolerance) should be nonnegative")
	}

	if t.Timeout < 0 {
		return errors.New("arg(timeout) should be nonnegative")
	}

	if t.Timeout == 0 {
		t.Timeout = 30
	}

	if t.Timeout > 3600*24 {
		return errors.New("arg(timeout) longer than one day")
	}

	t.Pause = strings.Replace(t.Pause, "，", ",", -1)
	t.Pause = strings.Replace(t.Pause, " ", "", -1)
	t.Args = strings.Replace(t.Args, "，", ",", -1)
	t.Tags = strings.Replace(t.Tags, "，", ",", -1)

	if t.Title == "" {
		return errors.New("arg(title) is required")
	}

	if str.Dangerous(t.Title) {
		return errors.New("arg(title) is dangerous")
	}

	if t.Script == "" {
		return errors.New("arg(script) is required")
	}

	if str.Dangerous(t.Args) {
		return errors.New("arg(args) is dangerous")
	}

	if str.Dangerous(t.Pause) {
		return errors.New("arg(pause) is dangerous")
	}

	if str.Dangerous(t.Tags) {
		return errors.New("arg(tags) is dangerous")
	}

	return nil
}

func (t *TaskTpl) Save(hosts []string) error {
	if err := t.CleanFields(); err != nil {
		return err
	}
	ctx := getCtx()
	// cnt, err := Count(DB().Model(&TaskTpl{}).Where("group_id=? and title=?", t.GroupId, t.Title))
	demo := &TaskTpl{}
	finder := zorm.NewSelectFinder(TaskTplStructTableName)
	finder.Append("Where group_id=? and title=?", t.GroupId, t.Title)
	_, err := zorm.QueryRow(ctx, finder, demo)
	if err != nil {
		return err
	}

	if demo.Id > 0 {
		return fmt.Errorf("task template already exists")
	}

	_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.Insert(ctx, t)
		if err != nil {
			return nil, err
		}

		entityMap := zorm.NewEntityMap(TaskTplHostStructTableName)
		entityMap.PkColumnName = "ii"
		entityMap.Set("id", t.Id)
		entityMap.Set("host", hosts)

		//执行
		_, err = zorm.InsertEntityMap(ctx, entityMap)
		return nil, err
	})

	return err

	// return DB().Transaction(func(tx *gorm.DB) error {
	// 	if err := tx.Create(t).Error; err != nil {
	// 		return err
	// 	}

	// 	for i := 0; i < len(hosts); i++ {
	// 		host := strings.TrimSpace(hosts[i])
	// 		if host == "" {
	// 			continue
	// 		}

	// 		err := tx.Table("task_tpl_host").Create(map[string]interface{}{
	// 			"id":   t.Id,
	// 			"host": host,
	// 		}).Error

	// 		if err != nil {
	// 			return err
	// 		}
	// 	}

	// 	return nil
	// })
}

func (t *TaskTpl) Hosts() ([]string, error) {
	arr := make([]string, 0)
	// err := DB().Table("task_tpl_host").Where("id=?", t.Id).Order("ii").Pluck("host", &arr).Error
	ctx := getCtx()
	finder := zorm.NewFinder().Append("select host FROM " + TaskTplHostStructTableName)
	finder.Append("Where id=?", t.Id)
	err := zorm.Query(ctx, finder, &arr, nil)
	return arr, err
}

func (t *TaskTpl) Update(hosts []string) error {
	if err := t.CleanFields(); err != nil {
		return err
	}

	// cnt, err := Count(DB().Model(&TaskTpl{}).Where("group_id=? and title=? and id <> ?", t.GroupId, t.Title, t.Id))
	ctx := getCtx()
	demo := &TaskTpl{}
	finder := zorm.NewSelectFinder(TaskTplStructTableName)
	finder.Append("Where group_id=? and title=? and id <> ?", t.GroupId, t.Title, t.Id)
	_, err := zorm.QueryRow(ctx, finder, demo)
	if err != nil {
		return err
	}

	if demo.Id > 0 {
		return fmt.Errorf("task template already exists")
	}

	_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		// err := tx.Model(t).Updates(map[string]interface{}{
		// 	"title":     t.Title,
		// 	"batch":     t.Batch,
		// 	"tolerance": t.Tolerance,
		// 	"timeout":   t.Timeout,
		// 	"pause":     t.Pause,
		// 	"script":    t.Script,
		// 	"args":      t.Args,
		// 	"tags":      t.Tags,
		// 	"account":   t.Account,
		// 	"update_by": t.UpdateBy,
		// 	"update_at": t.UpdateAt,
		// }).Error
		_, err := zorm.UpdateNotZeroValue(ctx, t)

		if err != nil {
			return nil, err
		}

		finder := zorm.NewDeleteFinder(TaskTplHostStructTableName)
		finder.Append("WHERE id=?", t.Id)
		_, err = zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}

		for i := 0; i < len(hosts); i++ {
			host := strings.TrimSpace(hosts[i])
			if host == "" {
				continue
			}

			// err := tx.Table("task_tpl_host").Create(map[string]interface{}{
			// 	"id":   t.Id,
			// 	"host": host,
			// }).Error
			entityMap := zorm.NewEntityMap(TaskTplHostStructTableName)
			entityMap.PkColumnName = "ii"
			entityMap.Set("id", t.Id)
			entityMap.Set("host", host)

			//执行
			_, err = zorm.InsertEntityMap(ctx, entityMap)
			return nil, err

			if err != nil {
				return nil, err
			}
		}

		return nil, err
	})
	return err

}

func (t *TaskTpl) Del() error {
	// return DB().Transaction(func(tx *gorm.DB) error {
	// 	if err := tx.Exec("DELETE FROM task_tpl_host WHERE id=?", t.Id).Error; err != nil {
	// 		return err
	// 	}

	// 	if err := tx.Delete(t).Error; err != nil {
	// 		return err
	// 	}

	// 	return nil
	// })
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(TaskTplHostStructTableName)
		finder.Append("WHERE id=?", t.Id)
		_, err := zorm.UpdateFinder(ctx, finder)

		_, err = zorm.Delete(ctx, t)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (t *TaskTpl) AddTags(tags []string, updateBy string) error {
	for i := 0; i < len(tags); i++ {
		if -1 == strings.Index(t.Tags, tags[i]+" ") {
			t.Tags += tags[i] + " "
		}
	}

	arr := strings.Fields(t.Tags)
	sort.Strings(arr)

	// return DB().Model(t).Updates(map[string]interface{}{
	// 	"tags":      strings.Join(arr, " ") + " ",
	// 	"update_by": updateBy,
	// 	"update_at": time.Now().Unix(),
	// }).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		//声明一个对象的指针,用于更新数据
		t.Tags = strings.Join(arr, " ") + " "
		t.UpdateBy = updateBy
		t.UpdateAt = time.Now().Unix()
		_, err := zorm.UpdateNotZeroValue(ctx, t)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (t *TaskTpl) DelTags(tags []string, updateBy string) error {
	for i := 0; i < len(tags); i++ {
		t.Tags = strings.ReplaceAll(t.Tags, tags[i]+" ", "")
	}

	// return DB().Model(t).Updates(map[string]interface{}{
	// 	"tags":      t.Tags,
	// 	"update_by": updateBy,
	// 	"update_at": time.Now().Unix(),
	// }).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		//声明一个对象的指针,用于更新数据
		t.UpdateBy = updateBy
		t.UpdateAt = time.Now().Unix()
		_, err := zorm.UpdateNotZeroValue(ctx, t)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (t *TaskTpl) UpdateGroup(groupId int64, updateBy string) error {
	// return DB().Model(t).Updates(map[string]interface{}{
	// 	"group_id":  groupId,
	// 	"update_by": updateBy,
	// 	"update_at": time.Now().Unix(),
	// }).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		//声明一个对象的指针,用于更新数据
		t.GroupId = groupId
		t.UpdateBy = updateBy
		t.UpdateAt = time.Now().Unix()
		_, err := zorm.UpdateNotZeroValue(ctx, t)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}
