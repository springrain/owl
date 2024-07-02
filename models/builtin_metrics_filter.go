package models

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
)

const MetricFilterTableName = "metric_filter"

type MetricFilter struct {
	zorm.EntityStruct
	ID             int64       `json:"id" column:"id"`
	Name           string      `json:"name" column:"name"`
	Configs        string      `json:"configs" column:"configs"`
	GroupsPerm     string      `json:"-" column:"groups_perm"`
	GroupsPermJson []GroupPerm `json:"groups_perm"`
	CreateAt       int64       `json:"create_at" column:"create_at"`
	CreateBy       string      `json:"create_by" column:"create_by"`
	UpdateAt       int64       `json:"update_at" column:"update_at"`
	UpdateBy       string      `json:"update_by" column:"update_by"`
}

type GroupPerm struct {
	Gid   int64 `json:"gid"`
	Write bool  `json:"write"` // write permission
}

func (f *MetricFilter) GetTableName() string {
	return MetricFilterTableName
}

func (f *MetricFilter) Verify() error {
	f.Name = strings.TrimSpace(f.Name)
	if f.Name == "" {
		return errors.New("name is blank")
	}
	f.Configs = strings.TrimSpace(f.Configs)
	if f.Configs == "" {
		return errors.New("configs is blank")
	}
	return nil
}

func (f *MetricFilter) Add(ctx *ctx.Context) error {
	if err := f.Verify(); err != nil {
		return err
	}
	now := time.Now().Unix()
	f.CreateAt = now
	f.UpdateAt = now
	if err := f.FE2DB(); err != nil {
		return err
	}
	return Insert(ctx, f)
}

func (f *MetricFilter) Update(ctx *ctx.Context) error {
	if err := f.Verify(); err != nil {
		return err
	}
	f.UpdateAt = time.Now().Unix()

	if err := f.FE2DB(); err != nil {
		return err
	}
	return Update(ctx, f, []string{"name", "configs", "groups_perm", "update_at", "update_by"})
	//return DB(ctx).Model(f).Select("name", "configs", "groups_perm", "update_at", "update_by").Updates(f).Error
}

func MetricFilterDel(ctx *ctx.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	return DeleteByIds(ctx, MetricFilterTableName, ids)
	//return DB(ctx).Where("id in ?", ids).Delete(new(MetricFilter)).Error
}

func MetricFilterGets(ctx *ctx.Context, where string, args ...interface{}) ([]MetricFilter, error) {
	//var lst []MetricFilter
	lst := make([]MetricFilter, 0)
	finder := zorm.NewSelectFinder(MetricFilterTableName)
	AppendWhere(finder, where, args...)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Where(where, args...).Find(&lst).Error
	for i := range lst {
		lst[i].DB2FE()
	}
	return lst, err
}

// get by id
func MetricFilterGet(ctx *ctx.Context, id int64) (*MetricFilter, error) {
	var f MetricFilter
	finder := zorm.NewSelectFinder(MetricFilterTableName).Append("WHERE id=?", id)
	_, err := zorm.QueryRow(ctx.Ctx, finder, &f)
	f.DB2FE()
	//err := DB(ctx).Where("id = ?", id).First(&f).Error
	return &f, err
}

func (f *MetricFilter) FE2DB() error {
	groupsPermBytes, err := json.Marshal(f.GroupsPermJson)
	if err != nil {
		return err
	}
	f.GroupsPerm = string(groupsPermBytes)

	return err
}
func (f *MetricFilter) DB2FE() error {
	err := json.Unmarshal([]byte(f.GroupsPerm), &f.GroupsPermJson)
	if err != nil {
		return err
	}
	return err
}
