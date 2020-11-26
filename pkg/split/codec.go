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
package split

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/util/rowcodec"

	"github.com/pingcap/tidb/util/codec"

	"github.com/pingcap/parser/model"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"

	"github.com/WentaoJin/tidba/pkg/db"
)

func decodeKeyFromString(s, statusAddr string, loc *time.Location) (string, error) {
	key, err := hex.DecodeString(s)
	if err != nil {
		return s, errors.Errorf("invalid record/index key: %X , appear error: %v", key, err)
	}
	// Auto decode byte if needed.
	_, bs, err := codec.DecodeBytes(key, nil)
	if err == nil {
		key = bs
	}
	tableID := tablecodec.DecodeTableID(key)
	if tableID == 0 {
		return s, errors.Errorf("invalid record/index key: %X , appear error: %v", key, err)
	}

	tblJson, err := getTableInfoSchema(statusAddr, tableID)
	if err != nil {
		return s, errors.Errorf("get table info schema failed: %v", err)
	}

	var tblInfo *model.TableInfo
	if err := json.Unmarshal([]byte(tblJson), &tblInfo); err != nil {
		return s, errors.Trace(errors.Errorf("json unmarshal table meta failed: %X", err))
	}

	if tablecodec.IsRecordKey(key) {
		ret, err := decodeRecordKey(key, tableID, tblInfo, loc)
		if err != nil {
			return s, err
		}
		return ret, nil
	}

	if tablecodec.IsIndexKey(key) {
		ret, err := decodeIndexKey(key, tableID, tblInfo, loc)
		if err != nil {
			return s, err
		}
		return ret, nil
	}
	return s, nil
}

func getTableInfoSchema(statusAddr string, tableID int64) (string, error) {
	response, err := http.Get(fmt.Sprintf("http://%s/schema?table_id=%v", statusAddr, tableID))
	if err != nil {
		return "", fmt.Errorf("http curl request get failed: %v", err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("read request data failed: %v", err)
	}
	return string(body), nil
}

func getSessionLocation(engine *db.Engine) (*time.Location, error) {
	query := `show variables  where Variable_name="time_zone"`
	_, res, err := engine.QuerySQL(query)
	if err != nil {
		return nil, fmt.Errorf("run SQL [%s] failed: %v", query, err)
	}
	if res[0]["VALUE"] == "SYSTEM" {
		query = `show variables where Variable_name="system_time_zone"`
		_, r, err := engine.QuerySQL(query)
		if err != nil {
			return nil, fmt.Errorf("run SQL [%s] failed: %v", query, err)
		}
		local, err := time.LoadLocation(r[0]["VALUE"])
		if err != nil {
			return nil, fmt.Errorf("time load location failed: %v", err)
		}
		return local, nil
	} else {
		local, err := time.LoadLocation(res[0]["VALUE"])
		if err != nil {
			return nil, fmt.Errorf("time load location failed: %v", err)
		}
		return local, nil
	}
}

func decodeRecordKey(key []byte, tableID int64, tblInfo *model.TableInfo, loc *time.Location) (string, error) {
	_, handle, err := tablecodec.DecodeRecordKey(key)
	if err != nil {
		return "", errors.Trace(err)
	}
	if handle.IsInt() {
		ret := make(map[string]interface{})
		ret["table_id"] = strconv.FormatInt(tableID, 10)
		ret["_tidb_rowid"] = handle.IntValue()
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
	if tblInfo != nil {
		// modify parts start
		//tblInfo := tbl.Meta()
		idxInfo := tables.FindPrimaryIndex(tblInfo)
		if idxInfo == nil {
			return "", errors.Trace(errors.Errorf("primary key not found when decoding record key: %X", key))
		}
		cols := make(map[int64]*types.FieldType, len(tblInfo.Columns))
		for _, col := range tblInfo.Columns {
			cols[col.ID] = &col.FieldType
		}
		handleColIDs := make([]int64, 0, len(idxInfo.Columns))
		for _, col := range idxInfo.Columns {
			handleColIDs = append(handleColIDs, tblInfo.Columns[col.Offset].ID)
		}

		datumMap, err := tablecodec.DecodeHandleToDatumMap(handle, handleColIDs, cols, loc, nil)
		if err != nil {
			return "", errors.Trace(err)
		}
		ret := make(map[string]interface{})
		ret["table_id"] = tableID
		handleRet := make(map[string]interface{})
		for colID, dt := range datumMap {
			dtStr, err := dt.ToString()
			if err != nil {
				return "", errors.Trace(err)
			}
			found := false
			for _, colInfo := range tblInfo.Columns {
				if colInfo.ID == colID {
					found = true
					handleRet[colInfo.Name.L] = dtStr
					break
				}
			}
			if !found {
				return "", errors.Trace(errors.Errorf("column not found when decoding record key: %X", key))
			}
		}
		ret["handle"] = handleRet
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
	ret := make(map[string]interface{})
	ret["table_id"] = tableID
	ret["handle"] = handle.String()
	retStr, err := json.Marshal(ret)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(retStr), nil
}

func decodeIndexKey(key []byte, tableID int64, tblInfo *model.TableInfo, loc *time.Location) (string, error) {
	// decode index key output with index column name
	if tblInfo != nil {
		_, indexID, _, err := tablecodec.DecodeKeyHead(key)
		if err != nil {
			return "", errors.Trace(errors.Errorf("invalid record/index key: %X", key))
		}
		//tblInfo := tbl.Meta()
		var colInfos []rowcodec.ColInfo
		var tps []*types.FieldType
		var targetIndex *model.IndexInfo
		for _, idx := range tblInfo.Indices {
			if idx.ID == indexID {
				targetIndex = idx
				colInfos = make([]rowcodec.ColInfo, 0, len(idx.Columns))
				tps = make([]*types.FieldType, 0, len(idx.Columns))
				for _, idxCol := range idx.Columns {
					col := tblInfo.Columns[idxCol.Offset]
					colInfos = append(colInfos, rowcodec.ColInfo{
						ID: col.ID,
						Ft: rowcodec.FieldTypeFromModelColumn(col),
					})
					tps = append(tps, rowcodec.FieldTypeFromModelColumn(col))
				}
				break
			}
		}
		if len(colInfos) == 0 || len(tps) == 0 || targetIndex == nil {
			return "", errors.Trace(errors.Errorf("index not found when decoding index key: %X", key))
		}
		values, err := tablecodec.DecodeIndexKV(key, []byte{0}, len(colInfos), tablecodec.HandleNotNeeded, colInfos)
		if err != nil {
			return "", errors.Trace(err)
		}
		ds := make([]types.Datum, 0, len(colInfos))
		for i := 0; i < len(colInfos); i++ {
			d, err := tablecodec.DecodeColumnValue(values[i], tps[i], loc)
			if err != nil {
				return "", errors.Trace(err)
			}
			ds = append(ds, d)
		}
		ret := make(map[string]interface{})
		ret["table_id"] = tableID
		ret["index_id"] = indexID
		idxValMap := make(map[string]interface{}, len(targetIndex.Columns))
		for i := 0; i < len(targetIndex.Columns); i++ {
			dtStr, err := ds[i].ToString()
			if err != nil {
				return "", errors.Trace(err)
			}
			idxValMap[targetIndex.Columns[i].Name.L] = dtStr
		}
		ret["index_vals"] = idxValMap
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	} else {
		_, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
		if err != nil {
			return "", errors.Trace(errors.Errorf("invalid index key: %X", key))
		}
		ret := make(map[string]interface{})
		ret["table_id"] = tableID
		ret["index_id"] = indexID
		ret["index_vals"] = strings.Join(indexValues, ", ")
		retStr, err := json.Marshal(ret)
		if err != nil {
			return "", errors.Trace(err)
		}
		return string(retStr), nil
	}
}
