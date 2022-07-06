package main

import (
	"encoding/json"
	"fmt"
	"github.com/MindTickle/platform-common/mtstore_helper"
	"strconv"
)

type UserEntityDbModel struct {
	mtstore_helper.BaseDbModel `json:"-"`
	CompanyId                  int64  `json:"company_id,string"`
	UserId                     int64  `json:"user_id,string"`
	EntityId                   int64  `json:"entity_id,string"`
	EntityVersion              int64  `json:"entity_version,string"`
	EntityType                 string `json:"entity_type"`
}

func (b *UserEntityDbModel) TableName() string {
	return "user_entity"
}

func (b *UserEntityDbModel) Id() string {
	return fmt.Sprintf("%s|%s",
		strconv.FormatInt(b.EntityId, 10),
		strconv.FormatInt(b.UserId, 16))
}

func (b *UserEntityDbModel) Doc() []byte {
	res, _ := json.Marshal(b)
	return res
}

type UserModule struct {
	UserId   string `json:"userId"`
	EntityId string `json:"entityId"`
}
