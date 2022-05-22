package models

import (
	"context"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/str"
)

const DashboardStructTableName = "dashboard"

// Dashboard
type Dashboard struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	//Id []
	Id int64 `column:"id" json:"id"`
	//GroupId busi group id
	GroupId int64 `column:"group_id" json:"group_id"`
	//Name []
	Name string `column:"name" json:"name"`
	//Tags split by space
	Tags string `column:"tags" json:"-"`
	//Configs dashboard variables
	Configs string `column:"configs" json:"configs"`
	//CreateAt []
	CreateAt int64 `column:"create_at" json:"create_at"`
	//CreateBy []
	CreateBy string `column:"create_by" json:"create_by"`
	//UpdateAt []
	UpdateAt int64 `column:"update_at" json:"update_at"`
	//UpdateBy []
	UpdateBy string `column:"update_by" json:"update_by"`
	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	TagsLst []string `json:"tags"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Dashboard) GetTableName() string {
	return DashboardStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Dashboard) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (d *Dashboard) Verify() error {
	if d.Name == "" {
		return errors.New("Name is blank")
	}

	if str.Dangerous(d.Name) {
		return errors.New("Name has invalid characters")
	}

	return nil
}

func (d *Dashboard) Add() error {
	if err := d.Verify(); err != nil {
		return err
	}

	exists, err := DashboardExists("group_id=? and name=?", d.GroupId, d.Name)
	if err != nil {
		return errors.WithMessage(err, "failed to count dashboard")
	}

	if exists {
		return errors.New("Dashboard already exists")
	}

	now := time.Now().Unix()
	d.CreateAt = now
	d.UpdateAt = now

	return Insert(d)

}

func (d *Dashboard) Update(selectField interface{}, selectFields ...interface{}) error {
	if err := d.Verify(); err != nil {
		return err
	}

	// return DB().Model(d).Select(selectField, selectFields...).Updates(d).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, d)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (d *Dashboard) Del() error {
	cgids, err := ChartGroupIdsOf(d.Id)
	if err != nil {
		return err
	}
	ctx := getCtx()
	if len(cgids) == 0 {
		_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			finder := zorm.NewDeleteFinder(ChartStructTableName)
			finder.Append("Where id=?", d.Id)
			_, err = zorm.UpdateFinder(ctx, finder)
			return nil, err
		})
		return err

		// return DB().Transaction(func(tx *gorm.DB) error {
		// 	if err := tx.Where("id=?", d.Id).Delete(&Dashboard{}).Error; err != nil {
		// 		return err
		// 	}
		// 	return nil
		// })
	}
	_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(ChartStructTableName)
		finder.Append("Where group_id in (?)", cgids)
		_, err = zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}

		finder = zorm.NewDeleteFinder(ChartGroupStructTableName)
		finder.Append("Where dashboard_id=?", d.Id)
		_, err = zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}

		finder = zorm.NewDeleteFinder(DashboardStructTableName)
		finder.Append("Where id=?", d.Id)
		_, err = zorm.UpdateFinder(ctx, finder)
		return nil, err

	})
	return err

	// return DB().Transaction(func(tx *gorm.DB) error {
	// 	if err := tx.Where("group_id in ?", cgids).Delete(&Chart{}).Error; err != nil {
	// 		return err
	// 	}

	// 	if err := tx.Where("dashboard_id=?", d.Id).Delete(&ChartGroup{}).Error; err != nil {
	// 		return err
	// 	}

	// 	if err := tx.Where("id=?", d.Id).Delete(&Dashboard{}).Error; err != nil {
	// 		return err
	// 	}

	// 	return nil
	// })
}

func DashboardGet(where string, args ...interface{}) (*Dashboard, error) {
	lst := make([]*Dashboard, 0)
	// err := DB().Where(where, args...).Find(&lst).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(DashboardStructTableName) // select * from t_demo
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

	lst[0].TagsLst = strings.Fields(lst[0].Tags)

	return lst[0], nil
}

func DashboardCount(where string, args ...interface{}) (num int64, err error) {
	// return Count(DB().Model(&Dashboard{}).Where(where, args...))
	finder := zorm.NewSelectFinder(DashboardStructTableName, "count(*)")
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	return Count(finder)
}

func DashboardExists(where string, args ...interface{}) (bool, error) {
	num, err := DashboardCount(where, args...)
	return num > 0, err
}

func DashboardGets(groupId int64, query string) ([]Dashboard, error) {
	ctx := getCtx()
	// session := DB().Where("group_id=?", groupId).Order("name")
	finder := zorm.NewSelectFinder(DashboardStructTableName) // select * from t_demo
	finder.Append("WHERE group_id=?", groupId).Append("Order by name")

	arr := strings.Fields(query)
	if len(arr) > 0 {
		for i := 0; i < len(arr); i++ {
			if strings.HasPrefix(arr[i], "-") {
				q := "%" + arr[i][1:] + "%"
				// session = session.Where("name not like ? and tags not like ?", q, q)
				finder.Append("AND name not like ? and tags not like ?", q, q)
			} else {
				q := "%" + arr[i] + "%"
				// session = session.Where("(name like ? or tags like ?)", q, q)
				finder.Append("AND name like ? or tags like ?)", q, q)
			}
		}
	}

	objs := make([]Dashboard, 0)
	// err := session.Select("id", "group_id", "name", "tags", "create_at", "create_by", "update_at", "update_by").Find(&objs).Error
	err := zorm.Query(ctx, finder, &objs, nil)
	if err == nil {
		for i := 0; i < len(objs); i++ {
			objs[i].TagsLst = strings.Fields(objs[i].Tags)
		}
	}

	return objs, err
}

func DashboardGetsByIds(ids []int64) ([]Dashboard, error) {
	if len(ids) == 0 {
		return []Dashboard{}, nil
	}

	lst := make([]Dashboard, 0)
	// err := DB().Where("id in ?", ids).Order("name").Find(&lst).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(DashboardStructTableName) // select * from t_demo
	finder.Append("WHERE id in (?)", ids).Append("Order by name")
	err := zorm.Query(ctx, finder, &lst, nil)

	return lst, err
}

func DashboardGetAll() ([]Dashboard, error) {
	var lst []Dashboard
	err := DB().Find(&lst).Error
	return lst, err
}
