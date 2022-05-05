package models

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"gitee.com/chunanyong/zorm"
	"github.com/pkg/errors"
	"github.com/toolkits/pkg/runner"
	"github.com/toolkits/pkg/str"
)

const ConfigsStructTableName = "configs"

// Configs
type Configs struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	//Id []
	Id   int64  `column:"id"`
	Ckey string `column:"ckey"`
	//Cval []
	Cval string `column:"cval"`
	//------------------数据库字段结束,自定义字段写在下面---------------//
	//如果查询的字段在column tag中没有找到,就会根据名称(不区分大小写,支持 _ 转驼峰)映射到struct的属性上

}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Configs) GetTableName() string {
	return ConfigsStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *Configs) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

// InitSalt generate random salt
func InitSalt() {
	val, err := ConfigsGet("salt")
	if err != nil {
		log.Fatalln("cannot query salt", err)
	}

	if val != "" {
		return
	}

	content := fmt.Sprintf("%s%d%d%s", runner.Hostname, os.Getpid(), time.Now().UnixNano(), str.RandLetters(6))
	salt := str.MD5(content)
	err = ConfigsSet("salt", salt)
	if err != nil {
		log.Fatalln("init salt in mysql", err)
	}
}

func ConfigsGet(ckey string) (string, error) {
	lst := make([]string, 0)
	ctx := getCtx()
	// err := DB().Model(&Configs{}).Where("ckey=?", ckey).Pluck("cval", &lst).Error
	finder := zorm.NewSelectFinder(ConfigsStructTableName, "cval")
	finder.Append("WHERE ckey=?", ckey)
	err := zorm.Query(ctx, finder, &lst, nil)
	if err != nil {
		return "", errors.WithMessage(err, "failed to query configs")
	}

	if len(lst) > 0 {
		return lst[0], nil
	}

	return "", nil
}

func ConfigsSet(ckey, cval string) error {
	ctx := getCtx()
	// num, err := Count(DB().Model(&Configs{}).Where("ckey=?", ckey))
	configs := &Configs{}
	finder := zorm.NewSelectFinder(ConfigsStructTableName)
	finder.Append("WHERE ckey=?", ckey)
	exists, err := zorm.QueryRow(ctx, finder, configs)

	if err != nil {
		return errors.WithMessage(err, "failed to count configs")
	}

	if !exists {
		_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			_, err := zorm.Insert(ctx, &Configs{
				Ckey: ckey,
				Cval: cval,
			})

			return nil, err
		})

	} else {
		// update
		// err = DB().Model(&Configs{}).Where("ckey=?", ckey).Update("cval", cval).Error
		_, err = zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
			finder := zorm.NewUpdateFinder(ConfigsStructTableName)
			finder.Append("ckey=?", ckey)
			_, err := zorm.UpdateFinder(ctx, finder)
			return nil, err
		})
	}

	return err
}

func ConfigsGets(ckeys []string) (map[string]string, error) {
	objs := make([]Configs, 0)
	// err := DB().Where("ckey in ?", ckeys).Find(&objs).Error
	ctx := getCtx()
	finder := zorm.NewSelectFinder(ConfigsStructTableName) // select * from t_demo
	//创建分页对象,查询完成后,page对象可以直接给前端分页组件使用
	finder.Append("Where ckey in (?)", ckeys)
	//执行查询
	err := zorm.Query(ctx, finder, &objs, nil)

	if err != nil {
		return nil, errors.WithMessage(err, "failed to gets configs")
	}

	count := len(ckeys)
	kvmap := make(map[string]string, count)
	for i := 0; i < count; i++ {
		kvmap[ckeys[i]] = ""
	}

	for i := 0; i < len(objs); i++ {
		kvmap[objs[i].Ckey] = objs[i].Cval
	}

	return kvmap, nil
}
