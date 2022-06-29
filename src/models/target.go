package models

import (
	"sort"
	"strings"
	"time"

	"context"

	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
)

const TargetStructTableName = "target"

// Target
type Target struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct

	Id       int64  `column:"id" json:"id"`
	GroupId  int64  `column:"group_id" json:"group_id"` //GroupId busi group id
	Cluster  string `column:"cluster" json:"cluster"`   //Cluster append to alert event as field
	Ident    string `column:"ident" json:"ident"`       //Ident target id
	Note     string `column:"note" json:"note"`         //Note append to alert event as field
	Tags     string `column:"tags" json:"-"`            //Tags append to series data as tags, split by space, append external space at suffix
	UpdateAt int64  `column:"update_at" json:"update_at"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	GroupObj *BusiGroup        `json:"group_obj"`
	TagsJSON []string          `json:"tags"`
	TagsMap  map[string]string `json:"-"` // internal use, append tags to series
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Target) GetTableName() string {
	return TargetStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Target) GetPKColumnName() string {
	//如果没有主键
	return "id"
}

func (t *Target) Add() error {
	obj, err := TargetGet("ident = ?", t.Ident)
	if err != nil {
		return err
	}

	if obj == nil {
		return Insert(t)
	}

	if obj.Cluster != t.Cluster {
		ctx := getCtx()
		finder := zorm.NewUpdateFinder(TargetStructTableName).Append("cluster=?,update_at=? WHERE ident=?", t.Cluster, t.UpdateAt, t.Ident)
		_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			_, err := zorm.UpdateFinder(ctx, finder)
			//如果返回的err不是nil,事务就会回滚
			return nil, err
		})
		return err
	}

	return nil
}

func (t *Target) FillGroup(cache map[int64]*BusiGroup) error {
	if t.GroupId <= 0 {
		return nil
	}

	bg, has := cache[t.GroupId]
	if has {
		t.GroupObj = bg
		return nil
	}

	bg, err := BusiGroupGetById(t.GroupId)
	if err != nil {
		return errors.WithMessage(err, "failed to get busi group")
	}

	t.GroupObj = bg
	cache[t.GroupId] = bg
	return nil
}

func TargetStatistics(cluster string) (*Statistics, error) {
	stats := make([]*Statistics, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(TargetStructTableName, "count(*) as total, max(update_at) as last_updated")
	if cluster != "" {
		finder.Append("Where cluster = ?", cluster)
	}
	err := zorm.Query(ctx, finder, &stats, nil)

	if err != nil {
		return nil, err
	}

	return stats[0], nil
}

func TargetDel(idents []string) error {
	if len(idents) == 0 {
		panic("idents empty")
	}
	// return DB().Where("ident in ?", idents).Delete(new(Target)).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(TargetStructTableName)
		finder.Append("WHERE ident in (?)", idents)
		_, err := zorm.UpdateFinder(ctx, finder)

		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err

}

func buildTargetWhere(bgid int64, clusters []string, query string) *zorm.Finder {
	finder := zorm.NewSelectFinder(TargetStructTableName).Append(" WHERE 1=1 ")

	if bgid >= 0 {
		finder.Append(" and group_id=?", bgid)
	}

	if len(clusters) > 0 {
		finder.Append(" and cluster in (?)", clusters)
	}

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			q := "%" + arr[i] + "%"
			finder.Append(" and (ident like ? or note like ? or tags like ?)", q, q, q)
		}
	}

	return finder
}

func TargetTotal(bgid int64, clusters []string, query string) (int64, error) {

	//构造查询用的finder
	finder := zorm.NewSelectFinder(TargetStructTableName, "count(*)")
	finder.Append("Where 1=1 ")
	if bgid >= 0 {
		finder.Append("And group_id=?", bgid)
	}

	if len(clusters) > 0 {
		finder.Append("And cluster in (?)", clusters)
	}

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			q := "%" + arr[i] + "%"
			// session = session.Where("ident like ? or note like ? or tags like ?", q, q, q)
			finder.Append("And (ident like ? or note like ? or tags like ?) ", q, q, q)
		}
	}
	return Count(finder)
	// return Count(buildTargetWhere(bgid, clusters, query))
}

func TargetGets(bgid int64, clusters []string, query string, limit, offset int) ([]*Target, error) {
	lst := make([]*Target, 0)
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(TargetStructTableName)
	page := zorm.NewPage()
	page.PageNo = offset/limit + 1 //查询第1页,默认是1
	page.PageSize = limit
	finder.Append("Where 1=1 ")
	if bgid >= 0 {
		finder.Append("And group_id=?", bgid)
	}

	if len(clusters) > 0 {
		finder.Append("And cluster in (?)", clusters)
	}

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			q := "%" + arr[i] + "%"
			finder.Append("And (ident like ? or note like ? or tags like ?)", q, q, q)
		}
	}
	finder.Append("Order by ident")

	err := zorm.Query(ctx, finder, &lst, page)

	// err := buildTargetWhere(bgid, clusters, query).Order("ident").Limit(limit).Offset(offset).Find(&lst).Error
	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].TagsJSON = strings.Fields(lst[i].Tags)
		}
	}
	return lst, err
}

func TargetGetsByCluster(cluster string) ([]*Target, error) {
	// session := DB().Model(&Target{})

	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(TargetStructTableName)
	if cluster != "" {
		// session = session.Where("cluster = ?", cluster)
		finder.Append("Where cluster = ?", cluster)
	}

	lst := make([]*Target, 0)
	// err := session.Find(&lst).Error
	err := zorm.Query(ctx, finder, &lst, nil)
	return lst, err
}

func TargetUpdateNote(idents []string, note string) error {
	// return DB().Model(&Target{}).Where("ident in ?", idents).Updates(map[string]interface{}{
	// 	"note":      note,
	// 	"update_at": time.Now().Unix(),
	// }).Error

	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewUpdateFinder(TargetStructTableName)
		finder.Append("note=?, update_at=?", note, time.Now().Unix()).Append("Where ident in (?)", idents)
		_, err := zorm.UpdateFinder(ctx, finder)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func TargetUpdateBgid(idents []string, bgid int64, clearTags bool) error {
	fields := map[string]interface{}{
		"group_id":  bgid,
		"update_at": time.Now().Unix(),
	}

	if clearTags {
		fields["tags"] = ""
	}

	// return DB().Model(&Target{}).Where("ident in ?", idents).Updates(fields).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewUpdateFinder(TargetStructTableName)
		finder.Append("group_id=?, update_at=?", bgid, time.Now().Unix()).Append("Where ident in (?)", idents)
		_, err := zorm.UpdateFinder(ctx, finder)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func TargetGet(where string, args ...interface{}) (*Target, error) {
	lst := make([]*Target, 0)
	// err := DB().Where(where, args...).Find(&lst).Error
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(TargetStructTableName) // select * from t_demo
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

	lst[0].TagsJSON = strings.Fields(lst[0].Tags)

	return lst[0], nil

}

func TargetGetById(id int64) (*Target, error) {
	return TargetGet("id = ?", id)
}

func TargetGetByIdent(ident string) (*Target, error) {
	return TargetGet("ident = ?", ident)
}

func TargetGetTags(idents []string) ([]string, error) {
	if len(idents) == 0 {
		return []string{}, nil
	}

	arr := make([]string, 0)
	// err := DB().Model(new(Target)).Where("ident in ?", idents).Select("distinct(tags) as tags").Pluck("tags", &arr).Error
	ctx := getCtx()
	finder := zorm.NewFinder().Append("select tags FROM " + TargetStructTableName)
	finder.Append("Where ident in (?)", idents)
	err := zorm.Query(ctx, finder, &arr, nil)
	if err != nil {
		return nil, err
	}

	cnt := len(arr)
	if cnt == 0 {
		return []string{}, nil
	}

	set := make(map[string]struct{})
	for i := 0; i < cnt; i++ {
		tags := strings.Fields(arr[i])
		for j := 0; j < len(tags); j++ {
			set[tags[j]] = struct{}{}
		}
	}

	cnt = len(set)
	ret := make([]string, 0, cnt)
	for key := range set {
		ret = append(ret, key)
	}

	sort.Strings(ret)

	return ret, err
}

func (t *Target) AddTags(tags []string) error {
	for i := 0; i < len(tags); i++ {
		if -1 == strings.Index(t.Tags, tags[i]+" ") {
			t.Tags += tags[i] + " "
		}
	}

	arr := strings.Fields(t.Tags)
	sort.Strings(arr)

	// return DB().Model(t).Updates(map[string]interface{}{
	// 	"tags":      strings.Join(arr, " ") + " ",
	// 	"update_at": time.Now().Unix(),
	// }).Error
	t.UpdateAt = time.Now().Unix()
	t.Tags = strings.Join(arr, " ") + " "
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, t)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err

}

func (t *Target) DelTags(tags []string) error {
	for i := 0; i < len(tags); i++ {
		t.Tags = strings.ReplaceAll(t.Tags, tags[i]+" ", "")
	}

	// return DB().Model(t).Updates(map[string]interface{}{
	// 	"tags":      t.Tags,
	// 	"update_at": time.Now().Unix(),
	// }).Error
	t.UpdateAt = time.Now().Unix()
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, t)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func TargetIdents(ids []int64) ([]string, error) {
	ret := make([]string, 0)

	if len(ids) == 0 {
		return ret, nil
	}

	// err := DB().Model(&Target{}).Where("id in ?", ids).Pluck("ident", &ret).Error
	ctx := getCtx()
	finder := zorm.NewFinder().Append("select ident FROM " + TargetStructTableName)
	finder.Append("Where id in (?)", ids)
	err := zorm.Query(ctx, finder, &ret, nil)
	return ret, err
}

func TargetIds(idents []string) ([]int64, error) {
	ret := make([]int64, 0)

	if len(idents) == 0 {
		return ret, nil
	}

	// err := DB().Model(&Target{}).Where("ident in ?", idents).Pluck("id", &ret).Error
	ctx := getCtx()
	finder := zorm.NewFinder().Append("select id FROM " + TargetStructTableName)
	finder.Append("Where ident in (?)", idents)
	err := zorm.Query(ctx, finder, &idents, nil)
	return ret, err
}

func IdentsFilter(idents []string, where string, args ...interface{}) ([]string, error) {
	arr := make([]string, 0)
	if len(idents) == 0 {
		return arr, nil
	}

	// err := DB().Model(&Target{}).Where("ident in ?", idents).Where(where, args...).Pluck("ident", &arr).Error
	ctx := getCtx()
	finder := zorm.NewFinder().Append("select ident FROM " + TargetStructTableName)
	finder.Append("Where ident in (?)", idents)
	if where != "" {
		finder.Append(" And "+where, args...)
	}
	err := zorm.Query(ctx, finder, &arr, nil)
	return arr, err
}
