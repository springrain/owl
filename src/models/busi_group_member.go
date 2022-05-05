package models

import (
	"context"

	"gitee.com/chunanyong/zorm"
)

const BusiGroupMemberStructTableName = "busi_group_member"

// BusiGroupMember
type BusiGroupMember struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	//Id []
	Id int64 `column:"id"`

	//BusiGroupId busi group id
	BusiGroupId int64 `column:"busi_group_id" json:"busi_group_id"`

	//UserGroupId user group id
	UserGroupId int64 `column:"user_group_id" json:"user_group_id"`

	//PermFlag ro | rw
	PermFlag string `column:"perm_flag" json:"perm_flag"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *BusiGroupMember) GetTableName() string {
	return BusiGroupMemberStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *BusiGroupMember) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (BusiGroupMember) TableName() string {
	return "busi_group_member"
}

func BusiGroupIds(userGroupIds []int64, permFlag ...string) ([]int64, error) {
	if len(userGroupIds) == 0 {
		return []int64{}, nil
	}
	ctx := getCtx()
	finder := zorm.NewSelectFinder(BusiGroupMemberStructTableName, "user_group_id")
	finder.Append("Where user_group_id in (?)", userGroupIds)
	// session := DB().Model(&BusiGroupMember{}).Where("user_group_id in ?", userGroupIds)
	if len(permFlag) > 0 {
		// session = session.Where("perm_flag=?", permFlag[0])
		finder.Append(" And perm_flag=?", permFlag[0])
	}

	ids := make([]int64, 0)
	// err := session.Pluck("busi_group_id", &ids).Error

	err := zorm.Query(ctx, finder, &ids, nil)

	return ids, err
}

func UserGroupIdsOfBusiGroup(busiGroupId int64, permFlag ...string) ([]int64, error) {
	// session := DB().Model(&BusiGroupMember{}).Where("busi_group_id = ?", busiGroupId)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(BusiGroupMemberStructTableName, "user_group_id")
	finder.Append("Where busi_group_id = ?", busiGroupId)
	if len(permFlag) > 0 {
		// session = session.Where("perm_flag=?", permFlag[0])
		finder.Append(" And perm_flag=?", permFlag[0])
	}

	ids := make([]int64, 0)
	// err := session.Pluck("user_group_id", &ids).Error
	err := zorm.Query(ctx, finder, &ids, nil)
	return ids, err
}

func BusiGroupMemberCount(where string, args ...interface{}) (int64, error) {
	// return Count(DB().Model(&BusiGroupMember{}).Where(where, args...))
	finder := zorm.NewSelectFinder(BusiGroupMemberStructTableName, "count(*)")
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	return Count(finder)
}

func BusiGroupMemberAdd(member BusiGroupMember) error {
	obj, err := BusiGroupMemberGet("busi_group_id = ? and user_group_id = ?", member.BusiGroupId, member.UserGroupId)
	if err != nil {
		return err
	}
	ctx := getCtx()

	if obj == nil {
		// insert
		// return Insert(&BusiGroupMember{
		// 	BusiGroupId: member.BusiGroupId,
		// 	UserGroupId: member.UserGroupId,
		// 	PermFlag:    member.PermFlag,
		// })
		_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			_, err := zorm.Insert(ctx, &BusiGroupMember{
				BusiGroupId: member.BusiGroupId,
				UserGroupId: member.UserGroupId,
				PermFlag:    member.PermFlag,
			})

			return nil, err
		})
		return err
	} else {
		// update
		if obj.PermFlag == member.PermFlag {
			return nil
		}
		_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			finder := zorm.NewUpdateFinder(BusiGroupMemberStructTableName)
			finder.Append("perm_flag", member.PermFlag).Append(" Where busi_group_id = ? and user_group_id = ?", member.BusiGroupId, member.UserGroupId)
			_, err := zorm.UpdateFinder(ctx, finder)
			//如果返回的err不是nil,事务就会回滚
			return nil, err
		})
		return err
		// return DB().Model(&BusiGroupMember{}).Where("busi_group_id = ? and user_group_id = ?", member.BusiGroupId, member.UserGroupId).Update("perm_flag", member.PermFlag).Error

	}
}

func BusiGroupMemberGet(where string, args ...interface{}) (*BusiGroupMember, error) {
	lst := make([]*BusiGroupMember, 0)
	// err := DB().Where(where, args...).Find(&lst).Error
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(BusiGroupMemberStructTableName) // select * from t_demo

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

func BusiGroupMemberDel(where string, args ...interface{}) error {
	// return DB().Where(where, args...).Delete(&BusiGroupMember{}).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(BusiGroupMemberStructTableName)
		finder.Append("Where "+where, args...)
		_, err := zorm.UpdateFinder(ctx, finder)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})

	return err

}

func BusiGroupMemberGets(where string, args ...interface{}) ([]BusiGroupMember, error) {
	lst := make([]BusiGroupMember, 0)
	// err := DB().Where(where, args...).Order("perm_flag").Find(&lst).Error
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(BusiGroupMemberStructTableName) // select * from t_demo
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	finder.Append("Order by perm_flag")
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return nil, err
	}
	return lst, err
}

func BusiGroupMemberGetsByBusiGroupId(busiGroupId int64) ([]BusiGroupMember, error) {
	return BusiGroupMemberGets("busi_group_id=?", busiGroupId)
}
