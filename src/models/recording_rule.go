package models

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
)

const RecordingRuleStructTableName = "recording_rule"

// A RecordingRule records its vector expression into new timeseries.
type RecordingRule struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	Id               int64  `column:"id" json:"id"`
	GroupId          int64  `column:"id" json:"group_id"`           // busi group id
	Cluster          string `column:"id" json:"cluster"`            // take effect by cluster
	Name             string `column:"id" json:"name"`               // new metric name
	Note             string `column:"id" json:"note"`               // note
	Disabled         int    `column:"id" json:"disabled"`           // 0: enabled, 1: disabled
	PromQl           string `column:"id" json:"prom_ql"`            // just one ql for promql
	PromEvalInterval int    `column:"id" json:"prom_eval_interval"` // unit:s
	AppendTags       string `column:"id" json:"-"`                  // split by space: service=n9e mod=api
	// for fe
	CreateAt int64  `column:"id" json:"create_at"`
	CreateBy string `column:"id" json:"create_by"`
	UpdateAt int64  `column:"id" json:"update_at"`
	UpdateBy string `column:"id" json:"update_by"`
	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上
	AppendTagsJSON []string `json:"append_tags"`
}

func (entity *RecordingRule) GetTableName() string {
	return RecordingRuleStructTableName
}

func (entity *RecordingRule) GetPKColumnName() string {
	return "id"
}

func (re *RecordingRule) FE2DB() {
	//re.Cluster = strings.Join(re.ClusterJSON, " ")
	re.AppendTags = strings.Join(re.AppendTagsJSON, " ")
}

func (re *RecordingRule) DB2FE() {
	//re.ClusterJSON = strings.Fields(re.Cluster)
	re.AppendTagsJSON = strings.Fields(re.AppendTags)
}
func (re *RecordingRule) Verify() error {
	if re.GroupId < 0 {
		return fmt.Errorf("GroupId(%d) invalid", re.GroupId)
	}

	if re.Cluster == "" {
		return errors.New("cluster is blank")
	}

	if !model.MetricNameRE.MatchString(re.Name) {
		return errors.New("Name has invalid chreacters")
	}

	if re.Name == "" {
		return errors.New("name is blank")
	}

	if re.PromEvalInterval <= 0 {
		re.PromEvalInterval = 60
	}

	re.AppendTags = strings.TrimSpace(re.AppendTags)
	rer := strings.Fields(re.AppendTags)
	for i := 0; i < len(rer); i++ {
		pair := strings.Split(rer[i], "=")
		if len(pair) != 2 || !model.LabelNameRE.MatchString(pair[0]) {
			return fmt.Errorf("AppendTags(%s) invalid", rer[i])
		}
	}

	return nil
}

func (re *RecordingRule) Add() error {
	if err := re.Verify(); err != nil {
		return err
	}

	exists, err := RecordingRuleExists(0, re.GroupId, re.Cluster, re.Name)
	if err != nil {
		return err
	}

	if exists {
		return errors.New("RecordingRule already exists")
	}

	now := time.Now().Unix()
	re.CreateAt = now
	re.UpdateAt = now

	return Insert(re)
}

func (re *RecordingRule) Update(ref RecordingRule) error {
	if re.Name != ref.Name {
		exists, err := RecordingRuleExists(re.Id, re.GroupId, re.Cluster, ref.Name)
		if err != nil {
			return err
		}
		if exists {
			return errors.New("RecordingRule already exists")
		}
	}

	ref.FE2DB()
	ref.Id = re.Id
	ref.GroupId = re.GroupId
	ref.CreateAt = re.CreateAt
	ref.CreateBy = re.CreateBy
	ref.UpdateAt = time.Now().Unix()
	err := ref.Verify()
	if err != nil {
		return err
	}

	//return DB().Model(re).Select("*").Updates(ref).Error
	ctx := getCtx()
	_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, re)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err

}

func (re *RecordingRule) UpdateFieldsMap(fields map[string]interface{}) error {
	//return DB().Model(re).Updates(fields).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {

		_, err := zorm.UpdateNotZeroValue(ctx, re)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func RecordingRuleDels(ids []int64, groupId int64) error {

	finder := zorm.NewDeleteFinder(RecordingRuleStructTableName).Append("WHERE id in(?) and group_id=?", ids, groupId)
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateFinder(ctx, finder)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	/*
		for i := 0; i < len(ids); i++ {
			ret := DB().Where("id = ? and group_id=?", ids[i], groupId).Delete(&RecordingRule{})
			if ret.Error != nil {
				return ret.Error
			}
		}
	*/
	return err
}

func RecordingRuleExists(id, groupId int64, cluster, name string) (bool, error) {
	ctx := getCtx()
	//session := DB().Where("id <> ? and group_id = ? and name =? ", id, groupId, name)
	finder := zorm.NewSelectFinder(RecordingRuleStructTableName).Append("WHERE id <> ? and group_id = ? and name =? ", id, groupId, name)
	lst := make([]RecordingRule, 0)
	//err := session.Find(&lst).Error
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return false, err
	}
	if len(lst) == 0 {
		return false, nil
	}

	// match cluster
	for _, r := range lst {
		if MatchCluster(r.Cluster, cluster) {
			return true, nil
		}
	}
	return false, nil
}

func RecordingRuleGets(groupId int64) ([]RecordingRule, error) {
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(RecordingRuleStructTableName).Append("WHERE group_id=? order by name asc", groupId)
	//session := DB().Where("group_id=?", groupId).Order("name")

	lst := make([]RecordingRule, 0)
	err := zorm.Query(ctx, finder, &lst, nil)
	//err := session.Find(&lst).Error
	if err == nil {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
	}

	return lst, err
}

func RecordingRuleGet(where string, regs ...interface{}) (*RecordingRule, error) {
	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(RecordingRuleStructTableName).Append("WHERE "+where, regs...)
	lst := make([]*RecordingRule, 0)
	//err := DB().Where(where, regs...).Find(&lst).Error
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return nil, err
	}

	if len(lst) == 0 {
		return nil, nil
	}

	lst[0].DB2FE()

	return lst[0], nil
}

func RecordingRuleGetById(id int64) (*RecordingRule, error) {
	return RecordingRuleGet("id=?", id)
}

func RecordingRuleGetsByCluster(cluster string) ([]*RecordingRule, error) {
	ctx := getCtx()
	//session := DB().Where("disabled = ? and prod = ?", 0, "")
	finder := zorm.NewSelectFinder(RecordingRuleStructTableName).Append("WHERE disabled = ? and prod = ?", 0, "")

	if cluster != "" {
		finder.Append(" and (cluster like ? or cluster like ?)", "%"+cluster+"%", "%"+ClusterAll+"%")
	}
	lst := make([]*RecordingRule, 0)
	//err := DB().Where(where, regs...).Find(&lst).Error
	err := zorm.Query(ctx, finder, &lst, nil)

	//var lst []*RecordingRule
	//err := session.Find(&lst).Error
	if err != nil {
		return lst, err
	}

	if len(lst) == 0 {
		return lst, nil
	}

	if cluster == "" {
		for i := 0; i < len(lst); i++ {
			lst[i].DB2FE()
		}
		return lst, nil
	}

	lr := make([]*RecordingRule, 0, len(lst))
	for _, r := range lst {
		if MatchCluster(r.Cluster, cluster) {
			r.DB2FE()
			lr = append(lr, r)
		}
	}

	return lr, err
}

func RecordingRuleStatistics(cluster string) (*Statistics, error) {
	/*
		session := DB().Model(&RecordingRule{}).Select("count(*) as total", "max(update_at) as last_updated")
		if cluster != "" {
			session = session.Where("cluster = ?", cluster)
		}
		var stats []*Statistics
		err := session.Find(&stats).Error
	*/

	ctx := getCtx()
	//构造查询用的finder
	finder := zorm.NewSelectFinder(RecordingRuleStructTableName, "count(*) as total,max(update_at) as last_updated").Append(" WHERE 1=1")
	if cluster != "" {
		// 简略的判断，当一个clustername是另一个clustername的substring的时候，会出现stats与预期不符，不影响使用
		finder.Append(" and (cluster like ? or cluster like ?)", "%"+cluster+"%", "%"+ClusterAll+"%")
	}
	stats := make([]*Statistics, 0)
	//err := DB().Where(where, regs...).Find(&lst).Error
	err := zorm.Query(ctx, finder, &stats, nil)

	if err != nil {
		return nil, err
	}

	return stats[0], nil
}
