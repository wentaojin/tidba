/*
Copyright © 2020 Marvin

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package sqlite

import (
	"encoding/json"
	"time"
)

type Entity struct {
	Comment   string    `gorm:"type:varchar(1000);comment:comment content" json:"comment"`
	CreatedAt time.Time `gorm:"<-:create" json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type Cluster struct {
	ID          uint64 `gorm:"primarykey;autoIncrement;comment:id" json:"id"`
	ClusterName string `gorm:"not null;type:varchar(120);uniqueIndex:uniq_clus_cluster_name;comment:name of cluster" json:"clusterName"`
	DbUser      string `gorm:"not null;type:varchar(100);comment:username of database" json:"dbUser"`
	DbPassword  string `gorm:"not null;type:varchar(100);comment:password of database" json:"dbPassword"`
	DbHost      string `gorm:"type:varchar(30);comment:host of database" json:"dbHost"`
	DbPort      uint64 `gorm:"type:int;comment:port of database" json:"dbPort"`
	DbCharset   string `gorm:"type:varchar(30);comment:charset of database" json:"dbCharset"`
	ConnParams  string `gorm:"type:varchar(60);comment:connect params of database" json:"connParams"`
	Path        string `gorm:"not null;type:varchar(120);comment:metadata of cluster" json:"path"`
	PrivateKey  string `gorm:"not null;type:varchar(120);comment:metadata of cluster" json:"privateKey"`
	*Entity
}

func (c *Cluster) String() string {
	val, _ := json.MarshalIndent(c, "", " ")
	return string(val)
}

type Inspect struct {
	ID            uint64 `gorm:"primarykey;autoIncrement;comment:id" json:"id"`
	ClusterName   string `gorm:"not null;type:varchar(120);uniqueIndex:uniq_insp_cluster_name;comment:name of cluster" json:"clusterName"`
	InspectConfig string `gorm:"not null;type:text;comment:config of cluster inspect" json:"inspectConfig"`
	*Entity
}

func (i *Inspect) String() string {
	val, _ := json.MarshalIndent(i, "", " ")
	return string(val)
}
