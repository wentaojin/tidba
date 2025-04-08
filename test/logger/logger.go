package main

import (
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	// 自定义时间格式化函数
	customTimeEncoder := func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
	}

	// 配置 zap 的 EncoderConfig
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = customTimeEncoder            // 设置时间格式化
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder // 日志级别大写

	// 创建一个 Console Encoder
	consoleEncoder := zapcore.NewConsoleEncoder(encoderConfig)

	// 创建一个核心（Core），将日志输出到标准输出
	core := zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zapcore.DebugLevel)

	// 创建 Logger
	logger := zap.New(core)
	defer logger.Sync() // 确保缓冲区中的日志被刷新

	// 使用 Sugar 封装 Logger
	// sugar := logger.Sugar()

	// 打印日志
	logger.Info("这是一条信息日志")
	logger.Warn("这是一条警告日志")
	logger.Error("这是一条错误日志")
}
