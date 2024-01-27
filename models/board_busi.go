package models

import (
	"context"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
)

const BoardBusigroupTableName = "board_busigroup"

type BoardBusigroup struct {
	// 引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	BusiGroupId int64 `json:"busi_group_id" column:"busi_group_id"`
	BoardId     int64 `json:"board_id" column:"board_id"`
}

func (bb *BoardBusigroup) GetTableName() string {
	return BoardBusigroupTableName
}

func (bb *BoardBusigroup) GetPKColumnName() string {
	// 如果没有主键
	return ""
}

func BoardBusigroupAdd(ctx context.Context, boardId int64, busiGroupIds []int64) error {
	if len(busiGroupIds) == 0 {
		return nil
	}
	boardBusigroups := make([]zorm.IEntityStruct, 0)
	for _, busiGroupId := range busiGroupIds {
		obj := BoardBusigroup{
			BusiGroupId: busiGroupId,
			BoardId:     boardId,
		}
		boardBusigroups = append(boardBusigroups, &obj)
		/*
			if err := tx.Create(obj).Error; err != nil {
				return err
			}
		*/
	}
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		return zorm.InsertSlice(ctx, boardBusigroups)
	})
	return err
}

func BoardBusigroupUpdate(ctx *ctx.Context, boardId int64, busiGroupIds []int64) error {

	_, err := zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(BoardBusigroupTableName).Append("WHERE board_id=?", boardId)
		id, err := zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return id, err
		}
		return nil, BoardBusigroupAdd(ctx, boardId, busiGroupIds)
	})
	return err

	/*
		return DB(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Where("board_id=?", boardId).Delete(&BoardBusigroup{}).Error; err != nil {
				return err
			}

			if err := BoardBusigroupAdd(tx, boardId, busiGroupIds); err != nil {
				return err
			}
			return nil
		})
	*/

}

func BoardBusigroupDelByBoardId(ctx *ctx.Context, boardId int64) error {
	_, err := zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(BoardBusigroupTableName).Append("WHERE board_id=?", boardId)
		return zorm.UpdateFinder(ctx, finder)
	})
	return err
	//return DB(ctx).Where("board_id=?", boardId).Delete(&BoardBusigroup{}).Error
}

// BoardBusigroupCheck(rt.Ctx, board.Id, bgids)
func BoardBusigroupCheck(ctx *ctx.Context, boardId int64, busiGroupIds []int64) (bool, error) {
	finder := zorm.NewSelectFinder(BoardBusigroupTableName, "COUNT(*)").Append("WHERE board_id=? and busi_group_id in (?)", boardId, busiGroupIds)
	//count, err := Count(ctx, finder)
	//count, err := Count(DB(ctx).Where("board_id=? and busi_group_id in (?)", boardId, busiGroupIds).Model(&BoardBusigroup{}))
	return Exists(ctx, finder)
}

func BoardBusigroupGets(ctx *ctx.Context) ([]BoardBusigroup, error) {
	objs := make([]BoardBusigroup, 0)
	finder := zorm.NewSelectFinder(BoardBusigroupTableName)
	err := zorm.Query(ctx.Ctx, finder, &objs, nil)
	//err := DB(ctx).Find(&objs).Error
	return objs, err
}

// get board ids by  busi group ids
func BoardIdsByBusiGroupIds(ctx *ctx.Context, busiGroupIds []int64) ([]int64, error) {
	ids := make([]int64, 0)
	finder := zorm.NewSelectFinder(BoardBusigroupTableName, "board_id").Append("WHERE busi_group_id in (?)", busiGroupIds)
	err := zorm.Query(ctx.Ctx, finder, &ids, nil)
	//err := DB(ctx).Model(&BoardBusigroup{}).Where("busi_group_id in (?)", busiGroupIds).Pluck("board_id", &ids).Error
	return ids, err
}
