package models

import (
	"errors"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
)

const BuiltinPayloadTableName = "builtin_payloads"

type BuiltinPayload struct {
	zorm.EntityStruct
	ID        int64  `json:"id" column:"id"`
	Type      string `json:"type" column:"type"`           // Alert Dashboard Collet
	Component string `json:"component" column:"component"` // Host MySQL Redis
	Cate      string `json:"cate" column:"cate"`           // categraf_v1 telegraf_v1
	Name      string `json:"name" column:"name"`           //
	Tags      string `json:"tags" column:"tags"`           // {"host":"
	Content   string `json:"content" column:"content"`
	UUID      int64  `json:"uuid" column:"uuid"`
	CreatedAt int64  `json:"created_at" column:"created_at"`
	CreatedBy string `json:"created_by" column:"created_by"`
	UpdatedAt int64  `json:"updated_at" column:"updated_at"`
	UpdatedBy string `json:"updated_by" column:"updated_by"`
}

func (bp *BuiltinPayload) GetTableName() string {
	return BuiltinPayloadTableName
}

func (bp *BuiltinPayload) Verify() error {
	bp.Type = strings.TrimSpace(bp.Type)
	if bp.Type == "" {
		return errors.New("type is blank")
	}

	bp.Component = strings.TrimSpace(bp.Component)
	if bp.Component == "" {
		return errors.New("component is blank")
	}

	if bp.Name == "" {
		return errors.New("name is blank")
	}

	return nil
}

func BuiltinPayloadExists(ctx *ctx.Context, bp *BuiltinPayload) (bool, error) {
	finder := zorm.NewSelectFinder(BuiltinPayloadTableName, "count(*)").Append("WHERE type = ? AND component = ? AND name = ? AND cate = ?", bp.Type, bp.Component, bp.Name, bp.Cate)
	count, err := Count(ctx, finder)
	//var count int64
	//err := DB(ctx).Model(bp).Where("type = ? AND component = ? AND name = ? AND cate = ?", bp.Type, bp.Component, bp.Name, bp.Cate).Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (bp *BuiltinPayload) Add(ctx *ctx.Context, username string) error {
	if err := bp.Verify(); err != nil {
		return err
	}
	exists, err := BuiltinPayloadExists(ctx, bp)
	if err != nil {
		return err
	}
	if exists {
		return errors.New("builtin payload already exists")
	}
	now := time.Now().Unix()
	bp.CreatedAt = now
	bp.CreatedBy = username
	bp.UpdatedAt = now
	bp.UpdatedBy = username
	return Insert(ctx, bp)
}

func (bp *BuiltinPayload) Update(ctx *ctx.Context, req BuiltinPayload) error {
	if err := req.Verify(); err != nil {
		return err
	}

	if bp.Type != req.Type || bp.Component != req.Component || bp.Name != req.Name {
		exists, err := BuiltinPayloadExists(ctx, &req)
		if err != nil {
			return err
		}
		if exists {
			return errors.New("builtin payload already exists")
		}
	}
	req.UpdatedAt = time.Now().Unix()
	req.UUID = bp.UUID
	req.CreatedBy = bp.CreatedBy
	req.CreatedAt = bp.CreatedAt

	return Update(ctx, &req, nil)
	//return DB(ctx).Model(bp).Select("*").Updates(req).Error
}

func BuiltinPayloadDels(ctx *ctx.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	return DeleteByIds(ctx, BuiltinPayloadTableName, ids)
	//return DB(ctx).Where("id in ?", ids).Delete(new(BuiltinPayload)).Error
}

func BuiltinPayloadGet(ctx *ctx.Context, where string, args ...interface{}) (*BuiltinPayload, error) {
	var bp BuiltinPayload
	finder := zorm.NewSelectFinder(BuiltinPayloadTableName)
	AppendWhere(finder, where, args...)
	has, err := zorm.QueryRow(ctx.Ctx, finder, &bp)
	//result := DB(ctx).Where(where, args...).Find(&bp)
	if err != nil {
		return nil, err
	}

	// 检查是否找到记录
	if !has {
		return nil, nil
	}

	return &bp, nil
}

func BuiltinPayloadGets(ctx *ctx.Context, typ, component, cate, query string) ([]*BuiltinPayload, error) {
	finder := zorm.NewSelectFinder(BuiltinPayloadTableName).Append("WHERE 1=1")
	//session := DB(ctx)
	if typ != "" {
		//session = session.Where("type = ?", typ)
		finder.Append("and type = ?", typ)
	}
	if component != "" {
		//session = session.Where("component = ?", component)
		finder.Append("and component = ?", component)
	}

	if cate != "" {
		//session = session.Where("cate = ?", cate)
		finder.Append("and cate = ?", cate)
	}

	if query != "" {
		arr := strings.Fields(query)
		for i := 0; i < len(arr); i++ {
			qarg := "%" + arr[i] + "%"
			//session = session.Where("name like ? or tags like ?", qarg, qarg)
			finder.Append("and (name like ? or tags like ?)", qarg, qarg)
		}
	}

	//var lst []*BuiltinPayload
	//err := session.Find(&lst).Error
	lst := make([]*BuiltinPayload, 0)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	return lst, err
}

// get cates of BuiltinPayload by type and component, return []string
func BuiltinPayloadCates(ctx *ctx.Context, typ, component string) ([]string, error) {
	var cates []string
	finder := zorm.NewSelectFinder(BuiltinPayloadTableName, "Distinct cate").Append("WHERE type = ? and component = ?", typ, component)
	err := zorm.Query(ctx.Ctx, finder, &cates, nil)
	//err := DB(ctx).Model(new(BuiltinPayload)).Where("type = ? and component = ?", typ, component).Distinct("cate").Pluck("cate", &cates).Error
	return cates, err
}

// get components of BuiltinPayload by type and cate, return string
func BuiltinPayloadComponents(ctx *ctx.Context, typ, cate string) (string, error) {
	var components []string
	finder := zorm.NewSelectFinder(BuiltinPayloadTableName, "Distinct component").Append("WHERE type = ? and cate = ?", typ, cate)
	err := zorm.Query(ctx.Ctx, finder, &components, nil)
	//err := DB(ctx).Model(new(BuiltinPayload)).Where("type = ? and cate = ?", typ, cate).Distinct("component").Pluck("component", &components).Error
	if err != nil {
		return "", err
	}

	if len(components) == 0 {
		return "", nil
	}
	return components[0], nil
}
