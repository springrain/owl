package models

import (
	"context"
	"fmt"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/ibex/server/config"
	"github.com/ccfos/nightingale/v6/models"
)

const TaskActionTableName = "task_action"

type TaskAction struct {
	zorm.EntityStruct
	Id     int64  `column:"id"`
	Action string `column:"action"`
	Clock  int64  `column:"clock"`
}

func (TaskAction) GetTableName() string {
	return TaskActionTableName
}

func TaskActionGet(where string, args ...interface{}) (*TaskAction, error) {
	var obj TaskAction
	finder := zorm.NewSelectFinder(TaskActionTableName).Append("WHERE "+where, args...)
	has, err := zorm.QueryRow(context.Background(), finder, &obj)
	//ret := DB().Where(where, args...).Find(&obj)
	if err != nil {
		return nil, err
	}

	if !has {
		return nil, nil
	}

	return &obj, nil
}

func TaskActionExistsIds(ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return ids, nil
	}

	var ret []int64
	finder := zorm.NewSelectFinder(TaskActionTableName, "id").Append("WHERE id in (?)", ids)
	_, err := zorm.QueryRow(context.Background(), finder, &ret)
	//err := DB().Model(&TaskAction{}).Where("id in ?", ids).Pluck("id", &ret).Error
	return ret, err
}

func CancelWaitingHosts(id int64) error {
	finder := zorm.NewUpdateFinder(tht(id)).Append(" status=? WHERE id = ? ", "cancelled", id)
	return models.UpdateFinder(NewN9eCtx(config.C.CenterApi), finder)
	//return DB().Table(tht(id)).Where("id = ? and status = ?", id, "waiting").Update("status", "cancelled").Error
}

func StartTask(id int64) error {
	finder := zorm.NewUpdateFinder(tht(id)).Append(" status=? WHERE id = ?", "", id)
	return models.UpdateFinder(NewN9eCtx(config.C.CenterApi), finder)
	//return DB().Model(&TaskScheduler{}).Where("id = ?", id).Update("scheduler", "").Error
}

func CancelTask(id int64) error {
	return CancelWaitingHosts(id)
}

func KillTask(id int64) error {
	if err := CancelWaitingHosts(id); err != nil {
		return err
	}

	now := time.Now().Unix()

	_, err := zorm.Transaction(context.Background(), func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewUpdateFinder(TaskHostDoingTableName).Append(" clock=?,action=? WHERE id = ? and action <> ?", now, "kill", id, "kill")
		_, err := zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}
		finder2 := zorm.NewUpdateFinder(tht(id)).Append(" status=? WHERE id = ? and status = ?", "killing", id, "running")
		return zorm.UpdateFinder(ctx, finder2)

	})

	return err

	/*
		return DB().Transaction(func(tx *gorm.DB) error {
			err := tx.Model(&TaskHostDoing{}).Where("id = ? and action <> ?", id, "kill").Updates(map[string]interface{}{
				"clock":  now,
				"action": "kill",
			}).Error
			if err != nil {
				return err
			}

			return tx.Table(tht(id)).Where("id = ? and status = ?", id, "running").Update("status", "killing").Error
		})
	*/
}

func (a *TaskAction) Update(action string) error {
	if !(action == "start" || action == "cancel" || action == "kill" || action == "pause") {
		return fmt.Errorf("action invalid")
	}

	_, err := zorm.Transaction(context.Background(), func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewUpdateFinder(TaskActionTableName).Append(" clock=?,action=? WHERE id = ?", time.Now().Unix(), action, a.Id)

		return zorm.UpdateFinder(ctx, finder)
	})

	/*
		err := DB().Model(a).Updates(map[string]interface{}{
			"action": action,
			"clock":  time.Now().Unix(),
		}).Error
	*/
	if err != nil {
		return err
	}

	if action == "start" {
		return StartTask(a.Id)
	}

	if action == "cancel" {
		return CancelTask(a.Id)
	}

	if action == "kill" {
		return KillTask(a.Id)
	}

	return nil
}

// LongTaskIds two weeks ago
func LongTaskIds() ([]int64, error) {
	clock := time.Now().Unix() - 604800*2
	var ids []int64
	finder := zorm.NewSelectFinder(TaskActionTableName, "id").Append("WHERE clock < ?", clock)
	_, err := zorm.QueryRow(context.Background(), finder, &ids)
	//err := DB().Model(&TaskAction{}).Where("clock < ?", clock).Pluck("id", &ids).Error
	return ids, err
}
