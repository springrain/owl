package models

import (
	"context"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
)

const TargetBusiGroupTableName = "target_busi_group"

type TargetBusiGroup struct {
	// 引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id          int64  `json:"id" column:"id"`
	TargetIdent string `json:"target_ident" column:"target_ident"`
	GroupId     int64  `json:"group_id" column:"group_id"`
	UpdateAt    int64  `json:"update_at" column:"update_at"`
}

func (t *TargetBusiGroup) GetTableName() string {
	return TargetBusiGroupTableName
}

func TargetBusiGroupsGetAll(ctx *ctx.Context) (map[string][]int64, error) {
	lst := make([]*TargetBusiGroup, 0)
	finder := zorm.NewSelectFinder(TargetBusiGroupTableName)
	finder.SelectTotalCount = false
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Find(&lst).Error
	if err != nil {
		return nil, err
	}
	tgs := make(map[string][]int64)
	for _, tg := range lst {
		tgs[tg.TargetIdent] = append(tgs[tg.TargetIdent], tg.GroupId)
	}
	return tgs, nil
}

func TargetGroupIdsGetByIdent(ctx *ctx.Context, ident string) ([]int64, error) {
	lst := make([]*TargetBusiGroup, 0)
	finder := zorm.NewSelectFinder(TargetBusiGroupTableName).Append("WHERE target_ident = ?", ident)
	finder.SelectTotalCount = false
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Where("target_ident = ?", ident).Find(&lst).Error
	if err != nil {
		return nil, err
	}
	groupIds := make([]int64, 0, len(lst))
	for _, tg := range lst {
		groupIds = append(groupIds, tg.GroupId)
	}
	return groupIds, nil
}

func TargetGroupIdsGetByIdents(ctx *ctx.Context, idents []string) ([]int64, error) {
	groupIds := make([]int64, 0)
	finder := zorm.NewSelectFinder(TargetBusiGroupTableName, " Distinct group_id ").Append("WHERE target_ident IN (?)", idents)
	err := zorm.Query(ctx.Ctx, finder, &groupIds, nil)
	if err != nil {
		return nil, err
	}
	return groupIds, nil
}

func TargetBindBgids(ctx *ctx.Context, idents []string, bgids []int64) error {
	lst := make([]zorm.IEntityStruct, 0, len(bgids)*len(idents))
	updateAt := time.Now().Unix()
	for _, bgid := range bgids {
		for _, ident := range idents {
			cur := TargetBusiGroup{
				TargetIdent: ident,
				GroupId:     bgid,
				UpdateAt:    updateAt,
			}
			lst = append(lst, &cur)
		}
	}

	_, err := zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		return zorm.InsertSlice(ctx, lst)
	})

	return err
	/*
		var cl clause.Expression = clause.Insert{Modifier: "ignore"}
		switch DB(ctx).Dialector.Name() {
		case "sqlite":
			cl = clause.Insert{Modifier: "or ignore"}
		case "postgres":
			cl = clause.OnConflict{DoNothing: true}
		}
		return DB(ctx).Clauses(cl).CreateInBatches(&lst, 10).Error
	*/
}

func TargetUnbindBgids(ctx *ctx.Context, idents []string, bgids []int64) error {
	finder := zorm.NewDeleteFinder(TargetBusiGroupTableName).Append("WHERE target_ident in (?) and group_id in (?)",
		idents, bgids)
	_, err := zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		return zorm.UpdateFinder(ctx, finder)
	})
	return err
}

func TargetDeleteBgids(ctx *ctx.Context, idents []string) error {
	finder := zorm.NewDeleteFinder(TargetBusiGroupTableName).Append("WHERE target_ident in (?)", idents)
	_, err := zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		return zorm.UpdateFinder(ctx, finder)
	})
	return err
}

func TargetOverrideBgids(ctx *ctx.Context, idents []string, bgids []int64) error {

	_, err := zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(TargetBusiGroupTableName).Append("WHERE target_ident in (?)", idents)
		_, err := zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}
		// 准备新的关联数据
		lst := make([]zorm.IEntityStruct, 0, len(bgids)*len(idents))
		updateAt := time.Now().Unix()
		for _, ident := range idents {
			for _, bgid := range bgids {
				cur := TargetBusiGroup{
					TargetIdent: ident,
					GroupId:     bgid,
					UpdateAt:    updateAt,
				}
				lst = append(lst, &cur)
			}
		}

		if len(lst) == 0 {
			return nil, nil
		}
		return zorm.InsertSlice(ctx, lst)
	})
	return err

}

func SeparateTargetIdents(ctx *ctx.Context, idents []string) (existing, nonExisting []string, err error) {
	existingMap := make(map[string]bool)

	finder := zorm.NewSelectFinder(TargetBusiGroupTableName, "Distinct target_ident").Append("WHERE target_ident IN (?)", idents)
	finder.SelectTotalCount = false
	err = zorm.Query(ctx.Ctx, finder, &existing, nil)
	if err != nil {
		return nil, nil, err
	}

	for _, ident := range existing {
		existingMap[ident] = true
	}

	// 分离不存在的 idents
	for _, ident := range idents {
		if !existingMap[ident] {
			nonExisting = append(nonExisting, ident)
		}
	}

	return
}
