package models

import (
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/ccfos/nightingale/v6/pkg/ctx"
)

const SsoConfigTableName = "sso_config"

type SsoConfig struct {
	zorm.EntityStruct
	Id       int64  `json:"id" column:"id"`
	Name     string `json:"name" column:"name"`
	Content  string `json:"content" column:"content"`
	UpdateAt int64  `json:"update_at" column:"update_at"`
}

func (b *SsoConfig) GetTableName() string {
	return SsoConfigTableName
}

// get all sso_config
func SsoConfigGets(ctx *ctx.Context) ([]SsoConfig, error) {
	lst := make([]SsoConfig, 0)
	finder := zorm.NewSelectFinder(SsoConfigTableName)
	err := zorm.Query(ctx.Ctx, finder, &lst, nil)
	//err := DB(ctx).Find(&lst).Error
	return lst, err
}

// 创建 builtin_cate
func (b *SsoConfig) Create(c *ctx.Context) error {
	return Insert(c, b)
}

func (b *SsoConfig) Update(c *ctx.Context) error {
	b.UpdateAt = time.Now().Unix()
	return Update(c, b, []string{"content", "update_at"})
	//return DB(c).Model(b).Select("content", "update_at").Updates(b).Error
}

// get sso_config last update time
func SsoConfigLastUpdateTime(c *ctx.Context) (int64, error) {
	var lastUpdateTime int64
	finer := zorm.NewSelectFinder(SsoConfigTableName, "max(update_at)")
	_, err := zorm.QueryRow(c.Ctx, finer, &lastUpdateTime)
	//err := DB(c).Model(&SsoConfig{}).Select("max(update_at)").Row().Scan(&lastUpdateTime)
	return lastUpdateTime, err
}

// get sso_config coutn by name
func SsoConfigCountByName(c *ctx.Context, name string) (int64, error) {
	finder := zorm.NewSelectFinder(SsoConfigTableName, "count(*)").Append("WHERE name = ?", name)
	return Count(c, finder)
	//var count int64
	//err := DB(c).Model(&SsoConfig{}).Where("name = ?", name).Count(&count).Error
	//return count, err
}
