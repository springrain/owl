package models

import (
	"context"
	"fmt"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
)

//BusiGroupStructTableName 表名常量,方便直接调用
const BusiGroupStructTableName = "busi_group"

// BusiGroup
type BusiGroup struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id          int64  `column:"id" json:"id"`
	Name        string `column:"name" json:"name"`
	LabelEnable int    `column:"label_enable" json:"label_enable"`
	LabelValue  string `column:"label_value" json:"label_value"` //LabelValue if label_enable: label_value can not be blank
	CreateAt    int64  `column:"create_at" json:"create_at"`
	CreateBy    string `column:"create_by" json:"create_by"`
	UpdateAt    int64  `column:"update_at" json:"update_at"`
	UpdateBy    string `column:"update_by" json:"update_by"`

	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	UserGroups []UserGroupWithPermFlag `json:"user_groups"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *BusiGroup) GetTableName() string {
	return BusiGroupStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *BusiGroup) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

type UserGroupWithPermFlag struct {
	UserGroup *UserGroup `json:"user_group"`
	PermFlag  string     `json:"perm_flag"`
}

func (bg *BusiGroup) FillUserGroups() error {
	members, err := BusiGroupMemberGetsByBusiGroupId(bg.Id)
	if err != nil {
		return err
	}

	if len(members) == 0 {
		return nil
	}

	for i := 0; i < len(members); i++ {
		ug, err := UserGroupGetById(members[i].UserGroupId)
		if err != nil {
			return err
		}
		bg.UserGroups = append(bg.UserGroups, UserGroupWithPermFlag{
			UserGroup: ug,
			PermFlag:  members[i].PermFlag,
		})
	}

	return nil
}

func BusiGroupGetMap() (map[int64]*BusiGroup, error) {
	lst := make([]*BusiGroup, 0)
	// err := DB().Find(&lst).Error

	ctx := getCtx()
	finder := zorm.NewSelectFinder(BusiGroupStructTableName)
	err := zorm.Query(ctx, finder, &lst, nil)

	if err != nil {
		return nil, err
	}

	ret := make(map[int64]*BusiGroup)
	for i := 0; i < len(lst); i++ {
		ret[lst[i].Id] = lst[i]
	}

	return ret, nil
}

func BusiGroupGet(where string, args ...interface{}) (*BusiGroup, error) {
	lst := make([]*BusiGroup, 0)
	// err := DB().Where(where, args...).Find(&lst).Error
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(BusiGroupStructTableName)
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

func BusiGroupGetById(id int64) (*BusiGroup, error) {
	return BusiGroupGet("id=?", id)
}

func BusiGroupExists(where string, args ...interface{}) (bool, error) {
	// num, err := Count(DB().Model(&BusiGroup{}).Where(where, args...))
	demo := &BusiGroup{}
	ctx := getCtx()
	finder := zorm.NewSelectFinder(BusiGroupStructTableName)
	if where != "" {
		finder.Append("Where "+where, args...)
	}
	has, err := zorm.QueryRow(ctx, finder, demo)
	return has, err
}

func (bg *BusiGroup) Del() error {
	// has, err := Exists(DB().Model(&AlertMute{}).Where("group_id=?", bg.Id))
	ctx := getCtx()
	finder := zorm.NewSelectFinder(AlertMuteStructTableName, "COUNT(*)")
	finder.Append("WHERE group_id=?", bg.Id)
	//查询条数
	num, err := Count(finder)
	if err != nil {
		return err
	}
	if num > 0 {
		return errors.New("Some alert mutes still in the BusiGroup")
	}

	// has, err = Exists(DB().Model(&AlertSubscribe{}).Where("group_id=?", bg.Id))
	finder = zorm.NewSelectFinder(AlertSubscribeStructTableName, "COUNT(*)")
	finder.Append("WHERE group_id=?", bg.Id)
	//查询条数
	num, err = Count(finder)
	if err != nil {
		return err
	}
	if num > 0 {
		return errors.New("Some alert subscribes still in the BusiGroup")
	}

	// has, err = Exists(DB().Model(&Target{}).Where("group_id=?", bg.Id))
	finder = zorm.NewSelectFinder(TargetStructTableName, "COUNT(*)")
	finder.Append("WHERE group_id=?", bg.Id)
	//查询条数
	num, err = Count(finder)
	if err != nil {
		return err
	}
	if num > 0 {
		return errors.New("Some targets still in the BusiGroup")
	}

	// has, err = Exists(DB().Model(&Dashboard{}).Where("group_id=?", bg.Id))
	finder = zorm.NewSelectFinder(DashboardStructTableName, "COUNT(*)")
	finder.Append("WHERE group_id=?", bg.Id)
	//查询条数
	num, err = Count(finder)
	if err != nil {
		return err
	}
	if num > 0 {
		return errors.New("Some dashboards still in the BusiGroup")
	}

	// has, err = Exists(DB().Model(&TaskTpl{}).Where("group_id=?", bg.Id))
	finder = zorm.NewSelectFinder(TaskTplStructTableName, "COUNT(*)")
	finder.Append("WHERE group_id=?", bg.Id)
	//查询条数
	num, err = Count(finder)
	if err != nil {
		return err
	}
	if num > 0 {
		return errors.New("Some recovery scripts still in the BusiGroup")
	}

	// hasCR, err := Exists(DB().Table("collect_rule").Where("group_id=?", bg.Id))
	finder = zorm.NewSelectFinder(AlertRuleStructTableName, "COUNT(*)")
	finder.Append("WHERE group_id=?", bg.Id)
	//查询条数
	num, err = Count(finder)
	if err != nil {
		return err
	}
	if num > 0 {
		return errors.New("Some alert rules still in the BusiGroup")
	}

	// return DB().Transaction(func(tx *gorm.DB) error {
	// 	if err := tx.Where("busi_group_id=?", bg.Id).Delete(&BusiGroupMember{}).Error; err != nil {
	// 		return err
	// 	}

	// 	if err := tx.Where("id=?", bg.Id).Delete(&BusiGroup{}).Error; err != nil {
	// 		return err
	// 	}

	// 	// 这个需要好好斟酌一下，删掉BG，对应的活跃告警事件也一并删除
	// 	// BG都删了，说明下面已经没有告警规则了，说明这些活跃告警永远都不会恢复了
	// 	// 而且这些活跃告警已经没人关心了，既然是没人关心的，删了吧
	// 	if err := tx.Where("group_id=?", bg.Id).Delete(&AlertCurEvent{}).Error; err != nil {
	// 		return err
	// 	}

	// 	return nil
	// })

	_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		finder := zorm.NewDeleteFinder(BusiGroupMemberStructTableName)
		finder.Append("Where busi_group_id=?", bg.Id)
		_, err = zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}

		finder = zorm.NewDeleteFinder(BusiGroupStructTableName)
		finder.Append("Where id=?", bg.Id)
		_, err = zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}

		finder = zorm.NewDeleteFinder(AlertCurEventStructTableName)
		finder.Append("Where group_id=?", bg.Id)
		_, err = zorm.UpdateFinder(ctx, finder)
		if err != nil {
			return nil, err
		}
		return nil, err
	})
	return err

}

func (bg *BusiGroup) AddMembers(members []BusiGroupMember, username string) error {
	for i := 0; i < len(members); i++ {
		err := BusiGroupMemberAdd(members[i])
		if err != nil {
			return err
		}
	}

	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		bg.UpdateAt = time.Now().Unix()
		bg.UpdateBy = username
		_, err := zorm.UpdateNotZeroValue(ctx, bg)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
	// return DB().Model(bg).Updates(map[string]interface{}{
	// 	"update_at": time.Now().Unix(),
	// 	"update_by": username,
	// }).Error
}

func (bg *BusiGroup) DelMembers(members []BusiGroupMember, username string) error {
	for i := 0; i < len(members); i++ {
		num, err := BusiGroupMemberCount("busi_group_id = ? and user_group_id <> ?", members[i].BusiGroupId, members[i].UserGroupId)
		if err != nil {
			return err
		}

		if num == 0 {
			// 说明这是最后一个user-group，如果再删了，就没人可以管理这个busi-group了
			return fmt.Errorf("The business group must retain at least one team")
		}

		err = BusiGroupMemberDel("busi_group_id = ? and user_group_id = ?", members[i].BusiGroupId, members[i].UserGroupId)
		if err != nil {
			return err
		}
	}

	// return DB().Model(bg).Updates(map[string]interface{}{
	// 	"update_at": time.Now().Unix(),
	// 	"update_by": username,
	// }).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		bg.UpdateAt = time.Now().Unix()
		bg.UpdateBy = username
		_, err := zorm.UpdateNotZeroValue(ctx, bg)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func (bg *BusiGroup) Update(name string, labelEnable int, labelValue string, updateBy string) error {
	if bg.Name == name && bg.LabelEnable == labelEnable && bg.LabelValue == labelValue {
		return nil
	}

	exists, err := BusiGroupExists("name = ? and id <> ?", name, bg.Id)
	if err != nil {
		return errors.WithMessage(err, "failed to count BusiGroup")
	}

	if exists {
		return errors.New("BusiGroup already exists")
	}
	if labelEnable == 1 {
		exists, err = BusiGroupExists("label_enable = 1 and label_value = ?  and id <> ?", labelValue, bg.Id)
		if err != nil {
			return errors.WithMessage(err, "failed to count BusiGroup")
		}

		if exists {
			return errors.New("BusiGroup already exists")
		}
	} else {
		labelValue = ""
	}

	// return DB().Model(bg).Updates(map[string]interface{}{
	// 	"name":      name,
	// 	"update_at": time.Now().Unix(),
	// 	"update_by": updateBy,
	// }).Error
	ctx := getCtx()
	_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		bg.Name = name
		bg.LabelEnable = labelEnable
		bg.LabelValue = labelValue
		bg.UpdateAt = time.Now().Unix()
		bg.UpdateBy = updateBy
		_, err := zorm.UpdateNotZeroValue(ctx, bg)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func BusiGroupAdd(name string, labelEnable int, labelValue string, members []BusiGroupMember, creator string) error {
	exists, err := BusiGroupExists("name=?", name)
	if err != nil {
		return errors.WithMessage(err, "failed to count BusiGroup")
	}

	if exists {
		return errors.New("BusiGroup already exists")
	}

	if labelEnable == 1 {
		exists, err = BusiGroupExists("label_enable = 1 and label_value = ?", labelValue)
		if err != nil {
			return errors.WithMessage(err, "failed to count BusiGroup")
		}

		if exists {
			return errors.New("BusiGroup already exists")
		}
	} else {
		labelValue = ""
	}

	count := len(members)
	for i := 0; i < count; i++ {
		ug, err := UserGroupGet("id=?", members[i].UserGroupId)
		if err != nil {
			return errors.WithMessage(err, "failed to get UserGroup")
		}

		if ug == nil {
			return errors.New("Some UserGroup id not exists")
		}
	}

	now := time.Now().Unix()
	obj := &BusiGroup{
		Name:        name,
		LabelEnable: labelEnable,
		LabelValue:  labelValue,
		CreateAt:    now,
		CreateBy:    creator,
		UpdateAt:    now,
		UpdateBy:    creator,
	}

	// return DB().Transaction(func(tx *gorm.DB) error {
	// 	if err := tx.Create(obj).Error; err != nil {
	// 		return err
	// 	}

	// 	for i := 0; i < len(members); i++ {
	// 		if err := tx.Create(&BusiGroupMember{
	// 			BusiGroupId: obj.Id,
	// 			UserGroupId: members[i].UserGroupId,
	// 			PermFlag:    members[i].PermFlag,
	// 		}).Error; err != nil {
	// 			return err
	// 		}
	// 	}

	// 	return nil
	// })
	ctx := getCtx()
	_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.Insert(ctx, obj)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(members); i++ {
			_, err := zorm.Insert(ctx, &BusiGroupMember{
				BusiGroupId: obj.Id,
				UserGroupId: members[i].UserGroupId,
				PermFlag:    members[i].PermFlag,
			})
			if err != nil {
				return nil, err
			}
		}
		return nil, err
	})
	return err
}

func BusiGroupStatistics() (*Statistics, error) {
	stats := make([]*Statistics, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(BusiGroupStructTableName, "count(*) as total, max(update_at) as last_updated")
	err := zorm.Query(ctx, finder, &stats, nil)

	if err != nil {
		return nil, err
	}

	return stats[0], nil
}
