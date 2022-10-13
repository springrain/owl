package models

import (
	"context"
	"time"
	"gitee.com/chunanyong/zorm"
) 

const AlertingEngineStructTableName = "alerting_engines"

type AlertingEngines struct {
	zorm.EntityStruct
	Id       int64  `column:"id" json:"id"`
	Instance string `column:"instance" json:"instance"`
	Cluster  string `column:"cluster" json:"cluster"` // reader cluster
	Clock    int64  `column:"clock" json:"clock"`
}

func (e *AlertingEngines) GetTableName() string {
	return AlertingEngineStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (e *AlertingEngines) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}
// UpdateCluster 页面上用户会给各个n9e-server分配要关联的目标集群是什么
func (e *AlertingEngines) UpdateCluster(c string) error {
	e.Cluster = c
	ctx := getCtx()
	// return DB().Model(e).Select("cluster").Updates(e).Error
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, e)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

// AlertingEngineGetCluster 根据实例名获取对应的集群名字
func AlertingEngineGetCluster(instance string) (string, error) {
	var objs []AlertingEngines
	ctx := getCtx()
	// err := DB().Where("instance=?", instance).Find(&objs).Error
	finder := zorm.NewSelectFinder(AlertingEngineStructTableName) // select * from t_demo
	finder.Append("Where instance=?", instance)
	err := zorm.Query(ctx, finder, &objs, nil)
	if err != nil {
		return "", err
	}

	if len(objs) == 0 {
		return "", nil
	}

	return objs[0].Cluster, nil
}

// AlertingEngineGets 拉取列表数据，用户要在页面上看到所有 n9e-server 实例列表，然后为其分配 cluster
func AlertingEngineGets(where string, args ...interface{}) ([]*AlertingEngines, error) {
	var objs []*AlertingEngines
	var err error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertingEngineStructTableName) // select * from t_demo
	// session := DB().Order("instance")
	finder.Append(" Order by instance")
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	err = zorm.Query(ctx, finder, &objs, nil)
	return objs, err
}

func AlertingEngineGet(where string, args ...interface{}) (*AlertingEngines, error) {
	lst, err := AlertingEngineGets(where, args...)
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}

func AlertingEngineGetsInstances(where string, args ...interface{}) ([]string, error) {
	var arr []string
	var err error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertingEngineStructTableName, "instance")
	// session := DB().Model(new(AlertingEngines)).Order("instance")
	if where != "" {
		// err = session.Where(where, args...)
		finder.Append("Where "+where, args...)
	}
	finder.Append(" Order by instance")
	err = zorm.Query(ctx, finder, &arr, nil)
	return arr, err
}

func AlertingEngineHeartbeat(instance, cluster string) error {
	var total int64
	ctx := getCtx()
	// err := DB().Model(new(AlertingEngines)).Where("instance=?", instance).Count(&total).Error
	finder := zorm.NewSelectFinder(AlertingEngineStructTableName, "count(*)")
	finder.Append("Where instance=?", instance)
	total, err := Count(finder)
	if err != nil {
		return err
	}

	if total == 0 {
		// insert
		// err = DB().Create(&AlertingEngines{
		// 	Instance: instance,
		// 	Clock:    time.Now().Unix(),
		// }).Error
		e := &AlertingEngines{
			Instance: instance,
			Cluster:  cluster,
			Clock:    time.Now().Unix(),
		}
		_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			_, err := zorm.Insert(ctx, e)
			return nil, err
		})
	} else {
		// update
		// fields := map[string]interface{}{"clock": time.Now().Unix(), "cluster": cluster}
		// err = DB().Model(new(AlertingEngines)).Where("instance=?", instance).Updates(fields).Error
		_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			finder = zorm.NewUpdateFinder(AlertingEngineStructTableName) // UPDATE t_demo SET
			finder.Append("clock=?, cluster=?", time.Now().Unix(), cluster).Append("WHERE instance=?", instance)
			_, err := zorm.UpdateFinder(ctx, finder)
			//如果返回的err不是nil,事务就会回滚
			return nil, err
		})
	}

	return err
}
