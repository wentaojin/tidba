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
package inspect

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"time"

	"github.com/wentaojin/tidba/database"
	"github.com/wentaojin/tidba/database/sqlite"
	"github.com/wentaojin/tidba/utils/cluster/operator"
	"github.com/wentaojin/tidba/utils/cluster/printer"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v3"
)

//go:embed template
var fs embed.FS

func GenClusterInspectReport(r *Report, file *os.File) error {
	/*
		To ensure that the table data content page wraps, the data splicing is required to be uniformly spliced ​​with the \n symbol, and the HTML CSS style format must have [word-wrap: break-word] and [white-space: pre-line]
		th, td {
		border: 1px solid #ddd;
		text-align: left;
		padding: 8px;
		word-wrap: break-word; 强制单词换行
		white-space: pre-line; 以换行符 \n 强制换行
		}
	*/
	tpl := template.New("inspection_cluster")

	tf, err := tpl.ParseFS(fs, "template/*.html")
	if err != nil {
		return fmt.Errorf("template parse FS failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_header", nil); err != nil {
		return fmt.Errorf("template FS Execute [report_header] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_body", r.ReportBody); err != nil {
		return fmt.Errorf("template FS Execute [report_body] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_summary", r.ReportSummary); err != nil {
		return fmt.Errorf("template FS Execute [report_summary] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_detail", r.ReportDetail); err != nil {
		return fmt.Errorf("template FS Execute [report_detail] template HTML failed: %v", err)
	}

	if err = tf.ExecuteTemplate(file, "report_footer", nil); err != nil {
		return fmt.Errorf("template FS Execute [report_footer] template HTML failed: %v", err)
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return nil
}

func GenClusterDevAndStatsAbnormalOutputExcel(fileName string, devPractices []*InspDevBestPracticesAbnormalOutput, dbStats []*InspDatabaseStatisticsAbnormalOutput) (bool, error) {
	if len(devPractices) == 0 && len(dbStats) == 0 {
		return false, nil
	}
	f := excelize.NewFile()

	var (
		devIndex   int
		err        error
		statsIndex int
	)

	if len(devPractices) > 0 {
		sheetName := "dev_best_practices"
		devIndex, err = f.NewSheet(sheetName)
		if err != nil {
			return false, err
		}

		headers := []string{"检查项", "检查类型", "检查级别", "检查 SQL", "检查对象", "异常项", "异常数", "备注"}

		var (
			headerFirst string
			headerLast  string
		)
		for i, header := range headers {
			col := string('A' + i)
			cellAddr := fmt.Sprintf("%s1", col)
			if i == 0 {
				headerFirst = cellAddr
			}
			if i == len(headers)-1 {
				headerLast = cellAddr
			}
			f.SetCellValue(sheetName, cellAddr, header)
		}

		titleStyle, err := f.NewStyle(&excelize.Style{
			Fill: excelize.Fill{
				Type:    "pattern",
				Pattern: 1,
				Color:   []string{"#4682B4"},
			},
			Font: &excelize.Font{
				Color:     "#000000",
				Bold:      true,
				VertAlign: "center",
			},
		})
		if err != nil {
			return false, err
		}

		f.SetCellStyle(sheetName, headerFirst, headerLast, titleStyle)

		var rows [][]interface{}

		for _, edp := range devPractices {
			var row []interface{}
			row = append(row, edp.CheckItem)
			row = append(row, edp.CheckCategory)
			row = append(row, edp.RectificationType)
			row = append(row, edp.CheckSql)
			row = append(row, edp.CheckType)
			row = append(row, edp.AbnormalDetail)
			row = append(row, edp.AbnormalCounts)
			row = append(row, edp.BestPracticeDesc)
			rows = append(rows, row)
		}

		for i, row := range rows {
			rowIndex := i + 2 // Start from the second row, because the first row is the title

			var (
				rowFirst string
				rowLast  string
			)
			for j, cell := range row {
				col := string('A' + j)
				cellAddress := fmt.Sprintf("%s%d", col, rowIndex)
				if j == 0 {
					rowFirst = cellAddress
				}
				if j == len(row)-1 {
					rowLast = cellAddress
				}
				f.SetCellValue(sheetName, cellAddress, cell)
			}

			if row[2].(string) == "强烈建议整改" {
				style, err := f.NewStyle(&excelize.Style{
					Fill: excelize.Fill{
						Type:    "pattern",
						Pattern: 1,
						Color:   []string{"#FF0000"},
					},
				})
				if err != nil {
					return false, err
				}
				f.SetCellStyle(sheetName, rowFirst, rowLast, style)
			}
		}
	}

	if len(dbStats) > 0 {
		sheetName := "database_statistics"
		statsIndex, err = f.NewSheet(sheetName)
		if err != nil {
			return false, err
		}

		headers := []string{"检查项", "检查标准", "检查 SQL", "异常项", "异常数", "备注"}

		var (
			headerFirst string
			headerLast  string
		)
		for i, header := range headers {
			col := string('A' + i)
			cellAddr := fmt.Sprintf("%s1", col)
			if i == 0 {
				headerFirst = cellAddr
			}
			if i == len(headers)-1 {
				headerLast = cellAddr
			}
			f.SetCellValue(sheetName, cellAddr, header)
		}

		titleStyle, err := f.NewStyle(&excelize.Style{
			Fill: excelize.Fill{
				Type:    "pattern",
				Pattern: 1,
				Color:   []string{"#4682B4"},
			},
			Font: &excelize.Font{
				Color:     "#000000",
				Bold:      true,
				VertAlign: "center",
			},
		})
		if err != nil {
			return false, err
		}

		f.SetCellStyle(sheetName, headerFirst, headerLast, titleStyle)

		var rows [][]interface{}

		for _, edp := range dbStats {
			var row []interface{}
			row = append(row, edp.CheckItem)
			row = append(row, edp.CheckStandard)
			row = append(row, edp.CheckSql)
			row = append(row, edp.AbnormalDetail)
			row = append(row, edp.AbnormalCounts)
			row = append(row, edp.Comment)
			rows = append(rows, row)
		}

		for i, row := range rows {
			rowIndex := i + 2 // Start from the second row, because the first row is the title
			for j, cell := range row {
				col := string('A' + j)
				cellAddress := fmt.Sprintf("%s%d", col, rowIndex)
				f.SetCellValue(sheetName, cellAddress, cell)
			}
		}
	}

	switch {
	case devIndex > 0 && statsIndex == 0:
		f.SetActiveSheet(devIndex)
	case devIndex == 0 && statsIndex > 0:
		f.SetActiveSheet(statsIndex)
	case devIndex > 0 && statsIndex > 0:
		f.SetActiveSheet(devIndex)
	default:
		return false, fmt.Errorf("the cluster dev and stats abnormal output excel failed: dev sheet index [%d] and stats sheet index [%d] unexpected", devIndex, statsIndex)
	}

	// remove origin
	if err := f.DeleteSheet("Sheet1"); err != nil {
		return false, err
	}
	if err := f.SaveAs(fileName); err != nil {
		return false, err
	}
	return true, f.Close()
}

func StartClusterInspect(ctx context.Context, clusterName string, l *printer.Logger, s, p *operator.SSHConnectionProps, gOpt *operator.Options) (*Report, error) {
	var (
		inspCfg *InspectConfig
	)

	db, err := database.Connector.GetDatabase(database.DefaultSqliteClusterName)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster [%s] database connector: %v", database.DefaultSqliteClusterName, err)
	}
	sqlite := db.(*sqlite.Database)

	sqinsp, err := sqlite.GetInspect(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("get inspect config: %v", err)
	}
	if err := yaml.Unmarshal([]byte(sqinsp.InspectConfig), &inspCfg); err != nil {
		return nil, fmt.Errorf("unmarshal inspect config: %v", err)
	}

	if inspCfg == nil {
		return nil, fmt.Errorf("cluster [%v] inspection configuration not found, please run [cluster inspect create -c {clusterName}] to create it first", clusterName)
	}

	clusterCfg, err := sqlite.GetCluster(ctx, clusterName)
	if err != nil {
		return nil, fmt.Errorf("get cluster config: %v", err)
	}

	sshDir, _ := filepath.Split(clusterCfg.PrivateKey)

	insp, err := NewInspector(ctx, clusterCfg.Path, clusterName, sshDir, inspCfg, l, s, p, gOpt)
	if err != nil {
		return nil, err
	}

	insp.GenInspectionWindow()

	if err := insp.InspClusterDatabaseVersion(); err != nil {
		return nil, err
	}

	rep := &ReportDetail{}

	if inspCfg.Modules.CheckHardwareInfo {
		basicHardware, err := insp.InspBasicHardwares()
		if err != nil {
			return nil, err
		}
		rep.BasicHardwares = basicHardware
	}

	if inspCfg.Modules.CheckSoftwareInfo {
		clusterSoftware, err := insp.InspClusterSoftware()
		if err != nil {
			return nil, err
		}
		rep.BasicSoftwares = clusterSoftware
	}

	rep.ClusterTopologys = insp.InspClusterTopology()

	if inspCfg.Modules.CheckTidbOverview {
		clusterSummary, err := insp.InspClusterSummary()
		if err != nil {
			return nil, err
		}
		rep.ClusterSummarys = clusterSummary
	}

	var (
		devAbnormalFlag    bool
		devAbnormalOutputs []*InspDevBestPracticesAbnormalOutput
	)
	if inspCfg.Modules.CheckDevBestPractices {
		var (
			devBestPractices []*DevBestPractice
			err              error
		)
		devBestPractices, devAbnormalFlag, devAbnormalOutputs, err = insp.InspDevBestPractices()
		if err != nil {
			return nil, err
		}
		rep.DevBestPractices = devBestPractices
	}

	if insp.inspConfig.Modules.CheckDbParams {
		dbVariable, err := insp.InspDatabaseVaribale()
		if err != nil {
			return nil, err
		}
		dbConfig, err := insp.InspDatabaseConfig()
		if err != nil {
			return nil, err
		}
		rep.DatabaseVaribales = dbVariable
		rep.DatabaseConfigs = dbConfig
	}

	var (
		statsAbnormalFlag    bool
		statsAbnormalOutputs []*InspDatabaseStatisticsAbnormalOutput
	)
	if insp.inspConfig.Modules.CheckStatsBestPractices {
		var (
			dbStatis []*DatabaseStatistics
			err      error
		)
		dbStatis, statsAbnormalFlag, statsAbnormalOutputs, err = insp.InspDatabaseStatistics()
		if err != nil {
			return nil, err
		}
		rep.DatabaseStatistics = dbStatis
	}

	if insp.inspConfig.Modules.CheckSysConfig {
		sysConfig, sysOutput, err := insp.InspSystemConfig()
		if err != nil {
			return nil, err
		}
		rep.SystemConfigs = sysConfig
		rep.SystemConfigOutputs = sysOutput
	}

	if inspCfg.Modules.CheckCrontab {
		sysCron, err := insp.InspSystemCrontab()
		if err != nil {
			return nil, err
		}
		rep.SystemCrontabs = sysCron
	}

	if inspCfg.Modules.CheckDmesgLogs {
		sysDmesg, err := insp.InspSystemDmesg()
		if err != nil {
			return nil, err
		}
		rep.SystemDmesgs = sysDmesg
	}
	if inspCfg.Modules.CheckDbErrorLogs {
		dbErrCount, err := insp.InspDatabaseErrorCount()
		if err != nil {
			return nil, err
		}
		rep.DatabaseErrorCounts = dbErrCount
	}

	if inspCfg.Modules.CheckUserSpace {
		schemaSpace, err := insp.InspDatabaseSchemaSpace()
		if err != nil {
			return nil, err
		}
		tableSpace, err := insp.InspDatabaseTableSpaceTop()
		if err != nil {
			return nil, err
		}
		rep.DatabaseSchemaSpaces = schemaSpace
		rep.DatabaseTableSpaceTops = tableSpace
	}

	if inspCfg.Modules.CheckPdPerformance {
		perfPd, err := insp.InspPerformanceStatisticsByPD()
		if err != nil {
			return nil, err
		}
		rep.PerformanceStatisticsByPds = perfPd
	}

	if inspCfg.Modules.CheckTidbPerformance {
		perfTidb, err := insp.InspPerformanceStatisticsByTiDB()
		if err != nil {
			return nil, err
		}
		rep.PerformanceStatisticsByTidbs = perfTidb
	}

	if inspCfg.Modules.CheckTikvPerformance {
		perfTikv, err := insp.InspPerformanceStatisticsByTiKV()
		if err != nil {
			return nil, err
		}
		rep.PerformanceStatisticsByTikvs = perfTikv
	}

	if inspCfg.Modules.CheckSQLOrderByElapsedTime {
		sqlElapsed, err := insp.InspSqlOrderedByElapsedTime()
		if err != nil {
			return nil, err
		}
		rep.SqlOrderedByElapsedTimes = sqlElapsed
	}

	sqlTiDBCpu, sqlTikvCpu, err := insp.InspSqlOrderedByComponentCpuTime(inspCfg.Modules.CheckSQLOrderByTidbCPUTime, inspCfg.Modules.CheckSQLOrderByTikvCPUTime)
	if err != nil {
		return nil, err
	}
	rep.SqlOrderedByTiDBCpuTimes = sqlTiDBCpu
	rep.SqlOrderedByTiKVCpuTimes = sqlTikvCpu

	if inspCfg.Modules.CheckSQLOrderByExecutions {
		sqlExecs, err := insp.InspSqlOrderedByExecutions()
		if err != nil {
			return nil, err
		}
		rep.SqlOrderedByExecutions = sqlExecs
	}

	if inspCfg.Modules.CheckSQLOrderByPlans {
		sqlPlans, err := insp.InspSqlOrderedByPlans()
		if err != nil {
			return nil, err
		}
		rep.SqlOrderedByPlans = sqlPlans
	}
	rep.InspectionWindowHour = divideFloatAndFormat(float64(inspCfg.WindowMinutes), 60)

	reportAbnormal := &ReportAbnormal{}
	if devAbnormalFlag {
		reportAbnormal.DevAbnormals = devAbnormalOutputs
	}
	if statsAbnormalFlag {
		reportAbnormal.StatsAbnormals = statsAbnormalOutputs
	}

	return &Report{
		ReportBody: &ReportBody{
			ClusterName:    clusterName,
			InspectionTime: time.Now().Format("2006-01-02 15:04:05"),
		},
		ReportSummary:  GenReportSummary(rep),
		ReportDetail:   rep,
		ReportAbnormal: reportAbnormal,
	}, nil
}
