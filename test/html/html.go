package main

import (
	"html/template"
	"os"
	"strings"
)

type TableData struct {
	Title   string
	Headers []string
	Rows    []Row
}

type Row struct {
	Columns []Column
}

type Column struct {
	Content string
}

func main() {
	tmpl := template.Must(template.New("table").Parse(`
<!DOCTYPE html>
<html>
<head>
    <title>{{.Title}}</title>
    <style>
        .table-container {
            width: 90%;
            margin: 2rem auto;
            font-family: Arial, sans-serif;
        }

        table {
            width: 100%;
            table-layout: fixed;
            border-collapse: collapse;
            box-shadow: 0 1px 3px rgba(0,0,0,0.12);
        }

        th, td {
            border: 1px solid #e0e0e0;
            padding: 12px;
            text-align: left;
            position: static;
        }

        .truncate {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .tooltip-wrapper {
            position: relative;
            display: inline-block;
            width: 100%;
            cursor: pointer;
        }

        .tooltip-bubble {
            visibility: hidden;
            position: absolute;
            background: #2d2d2d;
            border: 1px solid #555;
            border-radius: 8px;
            color: #fff;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 1000;
            max-width: min(calc(100vw - 40px), 800px);
            min-width: 200px;
            bottom: calc(100% + 15px);
            left: 50%;
            transform: translateX(-50%);
            opacity: 0;
            padding: 10px;
            overflow-y: auto;
            transition: opacity 0.2s;
            width: 800px;
            white-space: normal;
            user-select: text;
            -webkit-user-select: text;
        }

        .tooltip-wrapper:hover .tooltip-bubble {
            visibility: visible;
            opacity: 1;
        }

        .tooltip-bubble.visible {
            visibility: visible;
            opacity: 1;
        }

        .tooltip-bubble::after {
            content: '';
            position: absolute;
            top: 100%;
            left: 50%;
            margin-left: -5px;
            border-width: 8px;
            border-style: solid;
            border-color: #2d2d2d transparent transparent transparent;
        }

        td:last-child .tooltip-bubble {
            left: auto;
            right: 0;
            transform: translateX(0);
            max-width: calc(100vw - 40px);
        }

        td:last-child .tooltip-bubble::after {
            left: auto;
            right: 15px;
        }
    </style>
    <script>
        document.addEventListener('click', function(e) {
            const tooltips = document.querySelectorAll('.tooltip-bubble');
            const clickedTooltip = e.target.closest('.tooltip-bubble');
            const clickedWrapper = e.target.closest('.tooltip-wrapper');

            // 点击弹窗内容时不处理
            if (clickedTooltip) return;

            // 点击触发区域
            if (clickedWrapper) {
                const tooltip = clickedWrapper.querySelector('.tooltip-bubble');
                const isVisible = tooltip.classList.contains('visible');
                
                // 关闭所有弹窗
                tooltips.forEach(t => t.classList.remove('visible'));
                
                // 切换当前弹窗状态
                if (!isVisible) {
                    tooltip.classList.add('visible');
                }
            } else {
                // 点击其他区域关闭所有弹窗
                tooltips.forEach(t => t.classList.remove('visible'));
            }
        });

        // 阻止弹窗内容点击事件冒泡
        document.querySelectorAll('.tooltip-bubble').forEach(tooltip => {
            tooltip.addEventListener('click', function(e) {
                e.stopPropagation();
            });
        });
    </script>
</head>
<body>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    {{range .Headers}}<th>{{.}}</th>{{end}}
                </tr>
            </thead>
            <tbody>
                {{range $rowIdx, $row := .Rows}}
                <tr>
                    {{range $colIdx, $col := $row.Columns}}
                    <td>
                        <div class="tooltip-wrapper">
                            <div class="truncate">{{$col.Content}}</div>
                            <div class="tooltip-bubble">
                                {{$col.Content}}
                            </div>
                        </div>
                    </td>
                    {{end}}
                </tr>
                {{end}}
            </tbody>
        </table>
    </div>
</body>
</html>
`))
	var s []string
	for i := 0; i < 10; i++ {
		s = append(s, "此订单包含详细的技术参数：CPU 3.5GHz, 内存32GB DDR4, 存储1TB NVMe SSD。物流信息：已通过航空运输发出，预计3个工作日内送达。")
	}
	// 测试数据
	data := TableData{
		Title:   "自适应弹窗表格演示",
		Headers: []string{"ID", "状态", "详细描述"},
		Rows: []Row{
			{
				Columns: []Column{
					{Content: "1001"},
					{Content: "已完成"},
					{Content: "异常报告：在2023-08-15 14:30:45 UTC检测到网络连接超时，重试机制已触发（3/5次），错误代码0x80004005，详细堆栈信息：..."},
				},
			},
			{
				Columns: []Column{
					{Content: "1002"},
					{Content: "处理中"},
					{Content: strings.Join(s, "\n")},
				},
			},
		},
	}

	f, err := os.Create("output.html")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	tmpl.Execute(f, data)
}
