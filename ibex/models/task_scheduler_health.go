package models

import (
	"context"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/ibex/server/config"
	"github.com/ccfos/nightingale/v6/models"
)

const TaskSchedulerHealthTableName = "task_scheduler_health"

type TaskSchedulerHealth struct {
	zorm.EntityStruct
	Scheduler string `column:"scheduler"`
	Clock     int64  `column:"clock"`
}

func (*TaskSchedulerHealth) GetTableName() string {
	return TaskSchedulerHealthTableName
}

func TaskSchedulerHeartbeat(scheduler string) error {
	finder := zorm.NewSelectFinder(TaskSchedulerHealthTableName, "count(*)").Append("WHERE scheduler = ?", scheduler)
	cnt, err := models.Count(NewN9eCtx(config.C.CenterApi), finder)
	//err := DB().Model(&TaskSchedulerHealth{}).Where("scheduler = ?", scheduler).Count(&cnt).Error
	if err != nil {
		return err
	}

	if cnt == 0 {
		err = models.Insert(NewN9eCtx(config.C.CenterApi), &TaskSchedulerHealth{
			Scheduler: scheduler,
			Clock:     time.Now().Unix(),
		})
		/*
			ret := DB().Create(&TaskSchedulerHealth{
				Scheduler: scheduler,
				Clock:     time.Now().Unix(),
			})
			err = ret.Error
		*/
	} else {
		finder := zorm.NewUpdateFinder(TaskSchedulerHealthTableName).Append("clock=? WHERE scheduler = ?", time.Now().Unix(), scheduler)
		err = models.UpdateFinder(NewN9eCtx(config.C.CenterApi), finder)
		//err = DB().Model(&TaskSchedulerHealth{}).Where("scheduler = ?", scheduler).Update("clock", time.Now().Unix()).Error
	}

	return err
}

func DeadTaskSchedulers() ([]string, error) {
	clock := time.Now().Unix() - 10
	arr := make([]string, 0)
	finder := zorm.NewSelectFinder(TaskSchedulerHealthTableName, "scheduler").Append("WHERE clock < ?", clock)
	err := zorm.Query(context.Background(), finder, &arr, nil)
	//err := DB().Model(&TaskSchedulerHealth{}).Where("clock < ?", clock).Pluck("scheduler", &arr).Error
	return arr, err
}

func DelDeadTaskScheduler(scheduler string) error {
	finder := zorm.NewDeleteFinder(TaskSchedulerHealthTableName).Append("WHERE scheduler = ?", scheduler)
	return models.UpdateFinder(NewN9eCtx(config.C.CenterApi), finder)
	//return DB().Where("scheduler = ?", scheduler).Delete(&TaskSchedulerHealth{}).Error
}
