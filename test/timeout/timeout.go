package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"
)

func main() {
	// 设置持续运行时间（单位：秒），0 表示无限运行
	duration := 10 // 可以修改为其他值，例如 10 表示运行 10 秒

	// 创建上下文和取消函数
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 如果持续时间大于 0，则设置超时
	if duration > 0 {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(ctx, time.Duration(duration)*time.Second)
		defer cancelFunc()
	}

	// 监听系统信号（如 Ctrl+C）
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt) // 捕获 Ctrl+C 信号

	// 启动主逻辑
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// 模拟程序运行逻辑
				fmt.Println("程序正在运行...")
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// 等待信号或超时
	select {
	case <-sigChan:
		cancel() // 手动取消上下文
		time.Sleep(10 * time.Second)
		fmt.Println("收到 Ctrl+C 信号，中断程序运行")
	case <-ctx.Done():
		// 超时或手动调用了 cancel
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Println("运行时间已到，自动结束程序")
		} else {
			fmt.Println("程序被取消")
		}
	}
}
