package models

import (
	"context"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/str"
)

const UserGroupStructTableName = "user_group"

type UserGroup struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id       int64   `column:"id" json:"id"`
	Name     string  `column:"name" json:"name"`
	Note     string  `column:"note" json:"note"`
	CreateAt int64   `column:"create_at" json:"create_at"`
	CreateBy string  `column:"create_by" json:"create_by"`
	UpdateAt int64   `column:"update_at" json:"update_at"`
	UpdateBy string  `column:"update_by" json:"update_by"`
	UserIds  []int64 `json:"-"`
}

func (entity *UserGroup) GetTableName() string {
	return UserGroupStructTableName
}

func (entity *UserGroup) GetPKColumnName() string {
	return "id"
}

func (ug *UserGroup) Verify() error {
	if str.Dangerous(ug.Name) {
		return errors.New("Name has invalid characters")
	}

	if str.Dangerous(ug.Note) {
		return errors.New("Note has invalid characters")
	}

	return nil
}

func (ug *UserGroup) Update(selectField interface{}, selectFields ...interface{}) error {
	if err := ug.Verify(); err != nil {
		return err
	}

	//return DB().Model(ug).Select(selectField, selectFields...).Updates(ug).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, ug)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func UserGroupCount(where string, args ...interface{}) (num int64, err error) {
	//return Count(DB().Model(&UserGroup{}).Where(where, args...))

	finder := zorm.NewSelectFinder(UserGroupStructTableName, "count(*)")
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	return Count(finder)
}

func (ug *UserGroup) Add() error {
	if err := ug.Verify(); err != nil {
		return err
	}

	num, err := UserGroupCount("name=?", ug.Name)
	if err != nil {
		return errors.WithMessage(err, "failed to count user-groups")
	}

	if num > 0 {
		return errors.New("UserGroup already exists")
	}

	now := time.Now().Unix()
	ug.CreateAt = now
	ug.UpdateAt = now
	return Insert(ug)
}

func (ug *UserGroup) Del() error {
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(UserGroupMemberStructTableName)
		finder.Append(" Where group_id=?", ug.Id)
		_, err := zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}
		finder2 := zorm.NewDeleteFinder(UserGroupStructTableName)
		finder2.Append(" Where id=?", ug.Id)
		_, err2 := zorm.UpdateFinder(ctx, finder2)

		return nil, err2
	})

	return err

}

func UserGroupGet(where string, args ...interface{}) (*UserGroup, error) {
	lst := make([]*UserGroup, 0)
	//err := DB().Where(where, args...).Find(&lst).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(UserGroupStructTableName)
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

	return lst[0], nil
}

func UserGroupGetById(id int64) (*UserGroup, error) {
	return UserGroupGet("id = ?", id)
}

func UserGroupGetByIds(ids []int64) ([]UserGroup, error) {
	lst := make([]UserGroup, 0)
	if len(ids) == 0 {
		return lst, nil
	}
	ctx := getCtx()
	finder := zorm.NewSelectFinder(UserGroupStructTableName)
	finder.Append("Where id in (?) ", ids).Append(" order by name asc ")
	err := zorm.Query(ctx, finder, &lst, nil)
	return lst, err
}

func UserGroupGetAll() ([]*UserGroup, error) {
	lst := make([]*UserGroup, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(UserGroupStructTableName)
	err := zorm.Query(ctx, finder, &lst, nil)
	return lst, err
}

func (ug *UserGroup) AddMembers(userIds []int64) error {
	count := len(userIds)
	for i := 0; i < count; i++ {
		user, err := UserGetById(userIds[i])
		if err != nil {
			return err
		}
		if user == nil {
			continue
		}
		err = UserGroupMemberAdd(ug.Id, user.Id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ug *UserGroup) DelMembers(userIds []int64) error {
	return UserGroupMemberDel(ug.Id, userIds)
}

func UserGroupStatistics() (*Statistics, error) {
	//session := DB().Model(&UserGroup{}).Select("count(*) as total", "max(update_at) as last_updated")
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(UserGroupStructTableName, "count(*) as total,max(update_at) as last_updated")
	stats := make([]*Statistics, 0)
	//err := session.Find(&stats).Error
	err := zorm.Query(ctx, finder, &stats, nil)

	if err != nil {
		return nil, err
	}

	return stats[0], nil
}
