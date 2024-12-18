package models

import (
	"context"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
	"github.com/ccfos/nightingale/v6/pkg/poster"

	"github.com/toolkits/pkg/logger"
)

const UserGroupMemberTableName = "user_group_member"

type UserGroupMember struct {
	// 引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id      int64 `column:"id"`
	GroupId int64 `column:"group_id"`
	UserId  int64 `column:"user_id"`
}

func (ugm *UserGroupMember) GetTableName() string {
	return UserGroupMemberTableName
}

func MyGroupIds(ctx *ctx.Context, userId int64) ([]int64, error) {
	ids := make([]int64, 0)
	finder := zorm.NewSelectFinder(UserGroupMemberTableName, "group_id").Append("WHERE user_id=?", userId)
	err := zorm.Query(ctx.Ctx, finder, &ids, nil)
	//err := DB(ctx).Model(&UserGroupMember{}).Where("user_id=?", userId).Pluck("group_id", &ids).Error
	return ids, err
}

// my business group ids
func MyBusiGroupIds(ctx *ctx.Context, userId int64) ([]int64, error) {
	groupIds, err := MyGroupIds(ctx, userId)
	if err != nil {
		return []int64{}, err
	}

	return BusiGroupIds(ctx, groupIds)
}

func MemberIds(ctx *ctx.Context, groupId int64) ([]int64, error) {
	ids := make([]int64, 0)
	finder := zorm.NewSelectFinder(UserGroupMemberTableName, "user_id").Append("WHERE group_id=?", groupId)
	err := zorm.Query(ctx.Ctx, finder, &ids, nil)
	//err := DB(ctx).Model(&UserGroupMember{}).Where("group_id=?", groupId).Pluck("user_id", &ids).Error
	return ids, err
}

func GroupsMemberIds(ctx *ctx.Context, groupIds []int64) ([]int64, error) {
	ids := make([]int64, 0)
	finder := zorm.NewSelectFinder(UserGroupMemberTableName, "user_id").Append("WHERE group_id in (?)", groupIds)
	finder.SelectTotalCount = false
	err := zorm.Query(ctx.Ctx, finder, &ids, nil)
	//err := DB(ctx).Model(&UserGroupMember{}).Where("group_id in ?", groupIds).Pluck("user_id", &ids).Error
	return ids, err
}

func UserGroupMemberCount(ctx *ctx.Context, where string, args ...interface{}) (int64, error) {
	finder := zorm.NewSelectFinder(UserGroupMemberTableName, "count(*)")
	AppendWhere(finder, where, args...)
	return Count(ctx, finder)
	//return Count(DB(ctx).Model(&UserGroupMember{}).Where(where, args...))
}

func UserGroupMemberAdd(ctx *ctx.Context, groupId, userId int64) error {
	num, err := UserGroupMemberCount(ctx, "user_id=? and group_id=?", userId, groupId)
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

	return Insert(ctx, &obj)
}

func UserGroupMemberDel(ctx *ctx.Context, groupId int64, userIds []int64) error {
	if len(userIds) == 0 {
		return nil
	}
	finder := zorm.NewDeleteFinder(UserGroupMemberTableName).Append("WHERE group_id = ? and user_id in (?)", groupId, userIds)
	return UpdateFinder(ctx, finder)
	//return DB(ctx).Where("group_id = ? and user_id in ?", groupId, userIds).Delete(&UserGroupMember{}).Error
}

func UserGroupMemberGetAll(ctx *ctx.Context) ([]*UserGroupMember, error) {
	if !ctx.IsCenter {
		lst, err := poster.GetByUrls[[]*UserGroupMember](ctx, "/v1/n9e/user-group-members")
		return lst, err
	}

	lst := make([]*UserGroupMember, 0)
	finder := zorm.NewSelectFinder(UserGroupMemberTableName)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Find(&lst).Error
	return lst, err
}

// UserGroupMemberSync Sync group information, incrementally adding or overwriting deletes
func UserGroupMemberSync(ctx *ctx.Context, ldapGids []int64, userId int64, coverTeams bool) error {
	if len(ldapGids) == 0 {
		if coverTeams {
			finder := zorm.NewDeleteFinder(UserGroupMemberTableName).Append("WHERE user_id = ?", userId)
			return UpdateFinder(ctx, finder)
			// If the user is not in any group, delete all the groups that the user is currently in
			//return DB(ctx).Where("user_id = ?", userId).Delete(&UserGroupMember{}).Error
		}

		return nil
	}

	// queries all the groups that the user is currently in
	curGids, err := MyGroupIds(ctx, userId)
	if err != nil {
		return err
	}

	curGidsCount := len(curGids)
	curGidSet := slice2Set(curGids)                         // All the current groups Set
	toInsert := make([]zorm.IEntityStruct, 0, curGidsCount) // Will be added

	// Prepare data for bulk insertion
	for _, gid := range ldapGids {
		if !curGidSet[gid] {
			// Add only groups where the user does not already exist
			toInsert = append(toInsert, &UserGroupMember{GroupId: gid, UserId: userId})
			curGidSet[gid] = true
		}
	}

	if len(toInsert) > 0 {
		_, err := zorm.Transaction(ctx.Ctx, func(ctx context.Context) (interface{}, error) {
			return zorm.InsertSlice(ctx, toInsert)
		})
		//err = DB(ctx).CreateInBatches(toInsert, 10).Error
		if err != nil {
			logger.Warningf("failed to insert user(%d) group member err: %+v", userId, err)
		}
	}

	if !coverTeams || len(curGids) == 0 {
		return nil
	}

	// 需要将用户在 ldap 中没有, n9e 中有的团队删除
	ldapGidSet := slice2Set(ldapGids)
	toDeleteIds := make([]int64, 0, curGidsCount)

	for _, gid := range curGids {
		if !ldapGidSet[gid] {
			toDeleteIds = append(toDeleteIds, gid)
			ldapGidSet[gid] = true
		}
	}

	if len(toDeleteIds) == 0 {
		return nil
	}
	finder := zorm.NewDeleteFinder(UserGroupMemberTableName).Append("WHERE user_id = ? AND group_id IN (?)", userId, toDeleteIds)
	return UpdateFinder(ctx, finder)
	//return DB(ctx).Where("user_id = ? AND group_id IN ?", userId, toDeleteIds).Delete(&UserGroupMember{}).Error
}

func UserGroupMemberSyncByUser(ctx *ctx.Context, user *User, coverTeams bool) error {
	if user == nil {
		return nil
	}

	return UserGroupMemberSync(ctx, user.TeamsLst, user.Id, coverTeams)
}

func slice2Set[T comparable](s []T) map[T]bool {
	m := make(map[T]bool, len(s))
	for _, item := range s {
		m[item] = true
	}

	return m
}
