package main

import (
	"fmt"

	"github.com/xuri/excelize/v2"
)

func main() {
	// 创建一个新的 Excel 文件
	f := excelize.NewFile()

	// 创建一个新的工作表
	sheetName := "Sheet1"
	index, _ := f.NewSheet(sheetName)

	// 设置标题行
	headers := []string{"ID", "Name", "Score"}

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
	// 创建标题行样式
	titleStyle, err := f.NewStyle(&excelize.Style{
		Fill: excelize.Fill{
			Type:    "pattern",
			Pattern: 1,
			Color:   []string{"#0000FF"}, // 蓝色背景
		},
		Font: &excelize.Font{
			Color:     "#FFFFFF", // 白色字体
			Bold:      true,
			VertAlign: "center",
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	// 应用标题行样式到第一行
	f.SetCellStyle(sheetName, headerFirst, headerLast, titleStyle)

	// 设置内容行
	rows := [][]interface{}{
		{1, "Alice", 85},
		{2, "Bob", 90},
		{3, "Charlie", 70},
		{4, "David", 95},
	}

	// 添加内容并根据条件设置行背景颜色
	for i, row := range rows {
		rowIndex := i + 2 // 从第二行开始，因为第一行是标题
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

		fmt.Println(rowFirst, rowLast)
		// 示例条件：如果Score大于90，则将整行标记为绿色
		if row[2].(int) > 90 {
			style, err := f.NewStyle(&excelize.Style{
				Fill: excelize.Fill{
					Type:    "pattern",
					Pattern: 1,
					Color:   []string{"#C6EFCE"},
				},
			})
			if err != nil {
				fmt.Println(err)
				return
			}
			f.SetCellStyle(sheetName, rowFirst, rowLast, style)
		}
	}

	// 设置工作表为活动工作表
	f.SetActiveSheet(index)

	// 保存文件
	if err := f.SaveAs("Book1.xlsx"); err != nil {
		fmt.Println(err)
	}
}
