package models

import (
	"context"

	"gitee.com/chunanyong/zorm"
)

const UserGroupMemberStructTableName = "user_group_member"

// UserGroupMemberStruct
type UserGroupMember struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	GroupId int64 `column:"group_id"`
	UserId  int64 `column:"user_id"`
}

func (entity *UserGroupMember) GetTableName() string {
	return UserGroupMemberStructTableName
}

func (entity *UserGroupMember) GetPKColumnName() string {
	//如果没有主键
	return ""
}

func MyGroupIds(userId int64) ([]int64, error) {
	ids := make([]int64, 0)
	// err := DB().Model(&UserGroupMember{}).Where("user_id=?", userId).Pluck("group_id", &ids).Error

	ctx := getCtx()
	finder := zorm.NewFinder().Append("select group_id FROM " + UserGroupMemberStructTableName)
	finder.Append("Where user_id=?", userId)
	err := zorm.Query(ctx, finder, &ids, nil)

	return ids, err
}

func MemberIds(groupId int64) ([]int64, error) {
	ids := make([]int64, 0)
	// err := DB().Model(&UserGroupMember{}).Where("group_id=?", groupId).Pluck("user_id", &ids).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(UserGroupMemberStructTableName, "user_id")
	finder.Append("Where group_id=?", groupId)
	err := zorm.Query(ctx, finder, &ids, nil)
	return ids, err
}

func UserGroupMemberCount(where string, args ...interface{}) (int64, error) {
	// return Count(DB().Model(&UserGroupMember{}).Where(where, args...))

	finder := zorm.NewSelectFinder(UserGroupMemberStructTableName, "count(*)") // select * from t_demo
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	return Count(finder)
}

func UserGroupMemberAdd(groupId, userId int64) error {
	num, err := UserGroupMemberCount("user_id=? and group_id=?", userId, groupId)
	if err != nil {
		return err
	}

	if num > 0 {
		// already exists
		return nil
	}

	obj := UserGroupMember{
		GroupId: groupId,
		UserId:  userId,
	}
	return Insert(&obj)
}

func UserGroupMemberDel(groupId int64, userIds []int64) error {
	if len(userIds) == 0 {
		return nil
	}

	// return DB().Where("group_id = ? and user_id in ?", groupId, userIds).Delete(&UserGroupMember{}).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(UserGroupMemberStructTableName)
		finder.Append("Where group_id = ? and user_id in (?)", groupId, userIds)
		_, err := zorm.UpdateFinder(ctx, finder)

		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})

	return err
}

func UserGroupMemberGetAll() ([]UserGroupMember, error) {
	lst := make([]UserGroupMember, 0)
	// err := DB().Find(&lst).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(UserGroupMemberStructTableName)
	//执行查询
	err := zorm.Query(ctx, finder, &lst, nil)
	return lst, err
}
