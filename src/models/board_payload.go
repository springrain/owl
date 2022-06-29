package models

import (
	"errors"
	"context"
	"gitee.com/chunanyong/zorm"
)

//BoardStructTableName 表名常量,方便直接调用
const BoardPayloadStructTableName = "board_payload"

type BoardPayload struct {
	//引入默认的struct,隔离IEntityStruct的方法改动
	zorm.EntityStruct
	//Id []
	Id      int64  `column:"id" json:"id"`
	Payload string `column:"payload" json:"payload"`
}

//GetTableName 获取表名称
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *BoardPayload) GetTableName() string {
	return BoardPayloadStructTableName
}

//GetPKColumnName 获取数据库表的主键字段名称.因为要兼容Map,只能是数据库的字段名称
//不支持联合主键,变通认为无主键,业务控制实现(艰难取舍)
//如果没有主键,也需要实现这个方法, return "" 即可
//IEntityStruct 接口的方法,实体类需要实现!!!
func (entity *BoardPayload) GetPKColumnName() string {
	//如果没有主键
	//return ""
	return "id"
}

func (p *BoardPayload) Update(selectField interface{}, selectFields ...interface{}) error {
	// return DB().Model(p).Select(selectField, selectFields...).Updates(p).Error
	ctx := getCtx()
	_, err := zorm.Transaction(ctx, func(ctx context.Context) (interface{}, error) {
		_, err := zorm.UpdateNotZeroValue(ctx, p)
		//如果返回的err不是nil,事务就会回滚
		return nil, err
	})
	return err
}

func BoardPayloadGets(ids []int64) ([]*BoardPayload, error) {
	if len(ids) == 0 {
		return nil, errors.New("empty ids")
	}

	// var arr []*BoardPayload
	// err := DB().Where("id in ?", ids).Find(&arr).Error
	arr := make([]*BoardPayload, 0)
	ctx := getCtx()
	finder := zorm.NewSelectFinder(BoardPayloadStructTableName) // select * from t_demo
	finder.Append("Where id in (?)", ids)
	err := zorm.Query(ctx, finder, &arr, nil)
	return arr, err
}

func BoardPayloadGet(id int64) (string, error) {
	payloads, err := BoardPayloadGets([]int64{id})
	if err != nil {
		return "", err
	}

	if len(payloads) == 0 {
		return "", nil
	}

	return payloads[0].Payload, nil
}

func BoardPayloadSave(id int64, payload string) error {
	// var bp BoardPayload
	// err := DB().Where("id = ?", id).Find(&bp).Error
	bp := BoardPayload{}
	ctx := getCtx()
	finder := zorm.NewSelectFinder(BoardPayloadStructTableName) // select * from t_demo
	finder.Append("Where id = ?", id)
	_, err := zorm.QueryRow(ctx, finder, &bp)
	if err != nil {
		return err
	}

	if bp.Id > 0 {
		// already exists
		bp.Payload = payload
		return bp.Update("payload")
	}

	return Insert(&BoardPayload{
		Id:      id,
		Payload: payload,
	})
}
