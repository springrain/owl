package models

import (
	"context"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
)

const MetricDescriptionStructTableName = "metric_description"

type MetricDescription struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id          int64  `column:"group_id" json:"id"`
	Metric      string `column:"metric" json:"metric"`
	Description string `column:"description" json:"description"`
	UpdateAt    int64  `column:"update_at" json:"update_at"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *MetricDescription) GetTableName() string {
	return MetricDescriptionStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *MetricDescription) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func MetricDescriptionUpdate(mds []MetricDescription) error {
	now := time.Now().Unix()

	for i := 0; i < len(mds); i++ {
		mds[i].Metric = strings.TrimSpace(mds[i].Metric)
		md, err := MetricDescriptionGet("metric = ?", mds[i].Metric)
		if err != nil {
			return err
		}

		if md == nil {
			// insert
			mds[i].UpdateAt = now
			err = Insert(&mds[i])
			if err != nil {
				return err
			}
		} else {
			// update
			err = md.Update(mds[i].Description, now)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (md *MetricDescription) Update(desn string, now int64) error {
	md.Description = desn
	md.UpdateAt = now

	//return DB().Model(md).Select("Description", "UpdateAt").Updates(md).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, md)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func MetricDescriptionGet(where string, args ...interface{}) (*MetricDescription, error) {
	lst := make([]*MetricDescription, 0)
	//err := DB().Where(where, args...).Find(&lst).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(MetricDescriptionStructTableName)
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

	return lst[0], nil
}

func MetricDescriptionTotal(query string) (int64, error) {
	finder := zorm.NewSelectFinder(MetricDescriptionStructTableName, "count(*)")
	if query != "" {
		q := "%" + query + "%"
		finder.Append(" WHERE metric like ? or description like ?", q, q)
	}
	return Count(finder)
}

func MetricDescriptionGets(query string, limit, offset int) ([]MetricDescription, error) {
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(MetricDescriptionStructTableName)
	page := zorm.NewPage()
	if offset == 0 {
		page.PageNo = offset + 1 //查询第1页,默认是1
	} else {
		page.PageNo = offset/limit + 1 //查询第1页,默认是1
	}
	page.PageSize = limit

	if query != "" {
		q := "%" + query + "%"
		finder.Append(" WHERE metric like ? or description like ?", q, q)
	}
	//session := DB().Order("metric").Limit(limit).Offset(offset)
	finder.Append(" Order by metric desc ")

	objs := make([]MetricDescription, 0)
	//err := session.Find(&objs).Error
	err := zorm.Query(ctx, finder, &objs, page)
	return objs, err
}

func MetricDescGetAll() ([]MetricDescription, error) {
	objs := make([]MetricDescription, 0)
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(MetricDescriptionStructTableName)
	err := zorm.Query(ctx, finder, &objs, nil)
	return objs, err
}

func MetricDescStatistics() (*Statistics, error) {
	//session := DB().Model(&MetricDescription{}).Select("count(*) as total", "max(update_at) as last_updated")
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(MetricDescriptionStructTableName, "count(*) as total,max(update_at) as last_updated")
	stats := make([]*Statistics, 0)
	//err := session.Find(&stats).Error
	err := zorm.Query(ctx, finder, &stats, nil)
	if err != nil {
		return nil, err
	}

	return stats[0], nil
}

func MetricDescriptionMapper(metrics []string) (map[string]string, error) {
	if len(metrics) == 0 {
		return map[string]string{}, nil
	}

	objs := make([]MetricDescription, 0)
	//err := DB().Where("metric in ?", metrics).Find(&objs).Error
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(MetricDescriptionStructTableName).Append(" WHERE metric in (?)", metrics)
	err := zorm.Query(ctx, finder, &objs, nil)
	if err != nil {
		return nil, err
	}

	count := len(objs)
	if count == 0 {
		return map[string]string{}, nil
	}

	mapper := make(map[string]string, count)
	for i := 0; i < count; i++ {
		mapper[objs[i].Metric] = objs[i].Description
	}

	return mapper, nil
}

func MetricDescriptionDel(ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	ctx := getCtx()
	finder := zorm.NewDeleteFinder(MetricDescriptionStructTableName).Append(" WHERE id in (?)", ids)
	_, err := zorm.UpdateFinder(ctx, finder)
	return err
}
