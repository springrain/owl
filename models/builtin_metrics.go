package models

import (
	"errors"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
)

const BuiltinMetricTableName = "builtin_metrics"

// BuiltinMetric represents a metric along with its metadata.
type BuiltinMetric struct {
	zorm.EntityStruct
	ID         int64  `json:"id" column:"id"`
	UUID       int64  `json:"uuid" column:"uuid"`
	Collector  string `json:"collector" column:"collector"` // Type of collector (e.g., 'categraf', 'telegraf')
	Typ        string `json:"typ" column:"typ"`             // Type of metric (e.g., 'host', 'mysql', 'redis')
	Name       string `json:"name" column:"name"`
	Unit       string `json:"unit" column:"unit"`
	Note       string `json:"note" column:"note"`
	Lang       string `json:"lang" column:"lang"`
	Expression string `json:"expression" column:"expression"`
	CreatedAt  int64  `json:"created_at" column:"created_at"`
	CreatedBy  string `json:"created_by" column:"created_by"`
	UpdatedAt  int64  `json:"updated_at" column:"updated_at"`
	UpdatedBy  string `json:"updated_by" column:"updated_by"`
}

func (bm *BuiltinMetric) GetTableName() string {
	return BuiltinMetricTableName
}

func (bm *BuiltinMetric) Verify() error {
	bm.Collector = strings.TrimSpace(bm.Collector)
	if bm.Collector == "" {
		return errors.New("collector is blank")
	}

	bm.Typ = strings.TrimSpace(bm.Typ)
	if bm.Typ == "" {
		return errors.New("type is blank")
	}

	bm.Name = strings.TrimSpace(bm.Name)
	if bm.Name == "" {
		return errors.New("name is blank")
	}

	return nil
}

func BuiltinMetricExists(ctx *ctx.Context, bm *BuiltinMetric) (bool, error) {
	finder := zorm.NewSelectFinder(BuiltinMetricTableName, "count(*)").Append("WHERE lang = ? and collector = ? and typ = ? and name = ?", bm.Lang, bm.Collector, bm.Typ, bm.Name)
	count, err := Count(ctx, finder)
	//var count int64
	//err := DB(ctx).Model(bm).Where("lang = ? and collector = ? and typ = ? and name = ?", bm.Lang, bm.Collector, bm.Typ, bm.Name).Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (bm *BuiltinMetric) Add(ctx *ctx.Context, username string) error {
	if err := bm.Verify(); err != nil {
		return err
	}
	// check if the builtin metric already exists
	exists, err := BuiltinMetricExists(ctx, bm)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("builtin metric already exists")
	}
	now := time.Now().Unix()
	bm.CreatedAt = now
	bm.UpdatedAt = now
	bm.UpdatedBy = username
	bm.CreatedBy = username
	return Insert(ctx, bm)
}

func (bm *BuiltinMetric) Update(ctx *ctx.Context, req BuiltinMetric) error {
	if err := req.Verify(); err != nil {
		return err
	}

	if bm.Lang != req.Lang && bm.Collector != req.Collector && bm.Typ != req.Typ && bm.Name != req.Name {
		exists, err := BuiltinMetricExists(ctx, &req)
		if err != nil {
			return err
		}
		if exists {
			return errors.New("builtin metric already exists")
		}
	}
	req.UpdatedAt = time.Now().Unix()
	req.CreatedAt = bm.CreatedAt
	req.CreatedBy = bm.CreatedBy
	req.Lang = bm.Lang
	req.UUID = bm.UUID

	return Update(ctx, &req, nil)

	//return DB(ctx).Model(bm).Select("*").Updates(req).Error
}

func BuiltinMetricDels(ctx *ctx.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}

	return DeleteByIds(ctx, BuiltinMetricTableName, ids)
	//return DB(ctx).Where("id in ?", ids).Delete(new(BuiltinMetric)).Error
}

func BuiltinMetricGets(ctx *ctx.Context, lang, collector, typ, query, unit string, limit, offset int) ([]*BuiltinMetric, error) {
	finder := zorm.NewSelectFinder(BuiltinMetricTableName).Append("WHERE 1=1")
	//session := DB(ctx)
	finder = builtinMetricQueryBuild(lang, collector, finder, typ, query, unit)
	finder.Append("order by collector asc, typ asc, name asc")
	lst := make([]*BuiltinMetric, 0)
	page := zorm.NewPage()
	page.PageSize = limit
	page.PageNo = offset / limit
	finder.SelectTotalCount = false

	err := zorm.Query(ctx.Ctx, finder, &lst, page)
	//var lst []*BuiltinMetric
	//err := session.Limit(limit).Offset(offset).Order("collector asc, typ asc, name asc").Find(&lst).Error
	return lst, err
}

func BuiltinMetricCount(ctx *ctx.Context, lang, collector, typ, query, unit string) (int64, error) {
	//session := DB(ctx).Model(&BuiltinMetric{})
	//session = builtinMetricQueryBuild(lang, collector, session, typ, query, unit)
	finder := zorm.NewSelectFinder(BuiltinMetricTableName, "count(*)").Append("WHERE 1=1")
	//session := DB(ctx)
	finder = builtinMetricQueryBuild(lang, collector, finder, typ, query, unit)
	//var cnt int64
	//err := session.Count(&cnt).Error
	cnt, err := Count(ctx, finder)
	return cnt, err
}

func builtinMetricQueryBuild(lang, collector string, finder *zorm.Finder, typ string, query, unit string) *zorm.Finder {
	if lang != "" {
		//session = session.Where("lang = ?", lang)
		finder.Append("and lang = ?", lang)
	}

	if collector != "" {
		//session = session.Where("collector = ?", collector)
		finder.Append("and collector = ?", collector)
	}

	if typ != "" {
		//session = session.Where("typ = ?", typ)
		finder.Append("and typ = ?", typ)
	}

	if unit != "" {
		us := strings.Split(unit, ",")
		//session = session.Where("unit in (?)", us)
		finder.Append("and unit in (?)", us)
	}

	if query != "" {
		qs := strings.Split(query, " ")

		for _, q := range qs {
			if strings.HasPrefix(q, "-") {
				q = strings.TrimPrefix(q, "-")
				queryPattern := "%" + q + "%"
				//session = session.Where("name NOT LIKE ? AND note NOT LIKE ? AND expression NOT LIKE ?", queryPattern, queryPattern, queryPattern)
				finder.Append("and name NOT LIKE ? AND note NOT LIKE ? AND expression NOT LIKE ?", queryPattern, queryPattern, queryPattern)
			} else {
				queryPattern := "%" + q + "%"
				//session = session.Where("name LIKE ? OR note LIKE ? OR expression LIKE ?", queryPattern, queryPattern, queryPattern)
				finder.Append("and (name LIKE ? OR note LIKE ? OR expression LIKE ?)", queryPattern, queryPattern, queryPattern)
			}
		}
	}
	return finder
}

func BuiltinMetricGet(ctx *ctx.Context, where string, args ...interface{}) (*BuiltinMetric, error) {
	//var lst []*BuiltinMetric
	//err := DB(ctx).Where(where, args...).Find(&lst).Error
	lst := make([]*BuiltinMetric, 0)
	finder := zorm.NewSelectFinder(BuiltinMetricTableName)
	AppendWhere(finder, where, args...)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	return lst[0], nil
}

func BuiltinMetricTypes(ctx *ctx.Context, lang, collector, query string) ([]string, error) {
	typs := make([]string, 0)
	finder := zorm.NewSelectFinder(BuiltinMetricTableName, "distinct typ").Append("WHERE 1=1")
	//session := DB(ctx).Model(&BuiltinMetric{})
	if lang != "" {
		//session = session.Where("lang = ?", lang)
		finder.Append("and lang = ?", lang)
	}

	if collector != "" {
		//session = session.Where("collector = ?", collector)
		finder.Append("and collector = ?", collector)
	}

	if query != "" {
		//session = session.Where("typ like ?", "%"+query+"%")
		finder.Append("and typ like ?", "%"+query+"%")
	}
	err := zorm.Query(ctx.Ctx, finder, &typs, nil)
	//err := session.Select("distinct(typ)").Pluck("typ", &typs).Error
	return typs, err
}

func BuiltinMetricCollectors(ctx *ctx.Context, lang, typ, query string) ([]string, error) {
	collectors := make([]string, 0)
	finder := zorm.NewSelectFinder(BuiltinMetricTableName, "distinct collector").Append("WHERE 1=1")

	//session := DB(ctx).Model(&BuiltinMetric{})
	if lang != "" {
		//session = session.Where("lang = ?", lang)
		finder.Append("and lang = ?", lang)
	}

	if typ != "" {
		//session = session.Where("typ = ?", typ)
		finder.Append("and typ = ?", typ)
	}

	if query != "" {
		//session = session.Where("collector like ?", "%"+query+"%")
		finder.Append("and collector like ?", "%"+query+"%")
	}
	err := zorm.Query(ctx.Ctx, finder, &collectors, nil)
	//err := session.Select("distinct(collector)").Pluck("collector", &collectors).Error
	return collectors, err
}

func BuiltinMetricBatchUpdateColumn(ctx *ctx.Context, col, old, new, updatedBy string) error {
	if old == new {
		return nil
	}

	finder := zorm.NewUpdateFinder(BuiltinMetricTableName)
	finder.Append(col+"=?,updated_by=? WHERE "+col+"=?", new, updatedBy, old)
	return UpdateFinder(ctx, finder)
	//return DB(ctx).Model(&BuiltinMetric{}).Where(fmt.Sprintf("%s = ?", col), old).Updates(map[string]interface{}{col: new, "updated_by": updatedBy}).Error
}
