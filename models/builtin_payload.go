package models

import (
	"context"
	"errors"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
)

const BuiltinPayloadTableName = "builtin_payloads"

type BuiltinPayload struct {
	zorm.EntityStruct
	ID          int64  `json:"id" column:"id"`
	Type        string `json:"type" column:"type"`                 // Alert Dashboard Collet
	Component   string `json:"component" column:"component"`       // Host MySQL Redis
	ComponentID int64  `json:"component_id" column:"component_id"` // Host MySQL Redis
	Cate        string `json:"cate" column:"cate"`                 // categraf_v1 telegraf_v1
	Name        string `json:"name" column:"name"`                 //
	Tags        string `json:"tags" column:"tags"`                 // {"host":"
	Content     string `json:"content" column:"content"`
	UUID        int64  `json:"uuid" column:"uuid"`
	CreatedAt   int64  `json:"created_at" column:"created_at"`
	CreatedBy   string `json:"created_by" column:"created_by"`
	UpdatedAt   int64  `json:"updated_at" column:"updated_at"`
	UpdatedBy   string `json:"updated_by" column:"updated_by"`
}

func (bp *BuiltinPayload) GetTableName() string {
	return BuiltinPayloadTableName
}

func (bp *BuiltinPayload) Verify() error {
	bp.Type = strings.TrimSpace(bp.Type)
	if bp.Type == "" {
		return errors.New("type is blank")
	}

	if bp.ComponentID == 0 {
		return errors.New("component_id is blank")
	}

	if bp.Name == "" {
		return errors.New("name is blank")
	}

	return nil
}

func BuiltinPayloadExists(ctx *ctx.Context, bp *BuiltinPayload) (bool, error) {
	finder := zorm.NewSelectFinder(BuiltinPayloadTableName, "count(*)").Append("WHERE type = ? AND component_id = ? AND name = ? AND cate = ?", bp.Type, bp.ComponentID, bp.Name, bp.Cate)
	count, err := Count(ctx, finder)
	//var count int64
	//err := DB(ctx).Model(bp).Where("type = ? AND component_id = ? AND name = ? AND cate = ?", bp.Type, bp.ComponentID, bp.Name, bp.Cate).Count(&count).Error
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

	if bp.Type != req.Type || bp.ComponentID != req.ComponentID || bp.Name != req.Name {
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

func BuiltinPayloadGets(ctx *ctx.Context, componentId int64, typ, cate, query string) ([]*BuiltinPayload, error) {
	finder := zorm.NewSelectFinder(BuiltinPayloadTableName).Append("WHERE 1=1")
	//session := DB(ctx)
	if typ != "" {
		//session = session.Where("type = ?", typ)
		finder.Append("and type = ?", typ)
	}
	if componentId != 0 {
		//session = session.Where("component_id = ?", componentId)
		finder.Append("and component_id = ?", componentId)
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
func BuiltinPayloadCates(ctx *ctx.Context, typ string, componentID uint64) ([]string, error) {
	var cates []string
	finder := zorm.NewSelectFinder(BuiltinPayloadTableName, "Distinct cate").Append("WHERE type = ? and component_id = ?", typ, componentID)
	err := zorm.Query(ctx.Ctx, finder, &cates, nil)
	//err := DB(ctx).Model(new(BuiltinPayload)).Where("type = ? and component_id = ?", typ, componentID).Distinct("cate").Pluck("cate", &cates).Error
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

// InitBuiltinPayloads 兼容新旧 BuiltinPayload 格式
func InitBuiltinPayloads(ctx *ctx.Context) error {
	//var lst []*BuiltinPayload
	lst := make([]*BuiltinPayload, 0)
	components, err := BuiltinComponentGets(ctx, "")
	if err != nil {
		return err
	}

	identToId := make(map[string]int64)
	for _, component := range components {
		identToId[component.Ident] = component.ID
	}

	finder := zorm.NewSelectFinder(BuiltinPayloadTableName).Append("WHERE component_id = 0 or component_id is NULL")
	finder.SelectTotalCount = false
	err = zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err = DB(ctx).Where("component_id = 0 or component_id is NULL").Find(&lst).Error

	if err != nil {
		return err
	}

	for _, bp := range lst {
		componentId, ok := identToId[bp.Component]
		if !ok {
			continue
		}
		bp.ComponentID = componentId
	}

	if len(lst) == 0 {
		return nil
	}
	listEntity := make([]zorm.IEntityStruct, len(lst))
	for i := 0; i < len(lst); i++ {
		listEntity = append(listEntity, lst[i])
	}
	_, err = zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		return zorm.InsertSlice(ctx, listEntity)
	})
	return err
	//return DB(ctx).Save(&lst).Error
}
