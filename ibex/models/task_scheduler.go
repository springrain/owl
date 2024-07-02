package models

import (
	"context"

	"gitee.com/chunanyong/zorm"
)

const TaskSchedulerTableName = "task_scheduler"

type TaskScheduler struct {
	zorm.EntityStruct
	Id        int64  `column:"id"`
	Scheduler string `column:"scheduler"`
}

func (*TaskScheduler) GetTableName() string {
	return TaskSchedulerTableName
}

func TasksOfScheduler(scheduler string) ([]int64, error) {
	ids := make([]int64, 0)
	finder := zorm.NewSelectFinder(TaskSchedulerTableName, "id").Append("WHERE scheduler = ?", scheduler)
	err := zorm.Query(context.Background(), finder, &ids, nil)
	//err := DB().Model(&TaskScheduler{}).Where("scheduler = ?", scheduler).Pluck("id", &ids).Error
	return ids, err
}

func TakeOverTask(id int64, pre, current string) (bool, error) {
	finder := zorm.NewUpdateFinder(TaskSchedulerTableName).Append("scheduler=? WHERE id = ? and scheduler = ?", current, id, pre)

	rowsAffected, err := zorm.Transaction(context.Background(), func(ctx context.Context) (interface{}, error) {
		return zorm.UpdateFinder(ctx, finder)
	})
	return rowsAffected.(int) > 0, err
	/*
		ret := DB().Model(&TaskScheduler{}).Where("id = ? and scheduler = ?", id, pre).Update("scheduler", current)
		if ret.Error != nil {
			return false, ret.Error
		}

		return ret.RowsAffected > 0, nil
	*/
}

func OrphanTaskIds() ([]int64, error) {
	ids := make([]int64, 0)
	finder := zorm.NewSelectFinder(TaskSchedulerTableName, "id").Append("WHERE scheduler = ?", "")
	err := zorm.Query(context.Background(), finder, &ids, nil)
	//err := DB().Model(&TaskScheduler{}).Where("scheduler = ''").Pluck("id", &ids).Error
	return ids, err
}

func CleanDoneTask(id int64) error {

	_, err := zorm.Transaction(context.Background(), func(ctx context.Context) (interface{}, error) {
		f1 := zorm.NewDeleteFinder(TaskSchedulerTableName).Append("WHERE id = ?", id)
		_, err := zorm.UpdateFinder(ctx, f1)
		if err != nil {
			return nil, err
		}
		f2 := zorm.NewDeleteFinder(TaskActionTableName).Append("WHERE id = ?", id)
		return zorm.UpdateFinder(ctx, f2)
	})

	return err
	/*
		return DB().Transaction(func(tx *gorm.DB) error {
			if err := tx.Where("id = ?", id).Delete(&TaskScheduler{}).Error; err != nil {
				return err
			}

			return tx.Where("id = ?", id).Delete(&TaskAction{}).Error
		})
	*/
}
