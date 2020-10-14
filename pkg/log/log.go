package log

import (
    "github.com/natefinch/lumberjack"
    "gitlab.mfwdev.com/paas/mfwregistry-k8sadapter/config"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "os"
)

var Logger *zap.SugaredLogger

func LoggerInit() (err error) {
    // make sure the path exists
    _ = os.MkdirAll(config.LogFilePath, 0755)
    // init logger
    writeSyncer := getLogWriter()
    encoder := getEncoder()
    core := zapcore.NewCore(encoder, writeSyncer, zapcore.Level(int8(config.LogLevel)))
    // the func call stack
    optionCallStack := zap.AddCaller()
    Logger = zap.New(core, optionCallStack).Sugar()

    //cfg := zap.Config{
    //    Level:            zap.NewAtomicLevelAt(zapcore.Level(int8(config.LogLevel))),
    //    Development:      true,
    //    Encoding:         "console", // console or json
    //    EncoderConfig:    zap.NewDevelopmentEncoderConfig(),
    //    OutputPaths:      []string{config.LogFilePath + "stdout.log"},
    //    ErrorOutputPaths: []string{config.LogFilePath + "error.log"}, //this setting only affects internal b-error, (runtime error)
    //}
    //cfg.Level.SetLevel(zapcore.Level(int8(c.Int("log-level"))))
    //logger, err := zap.NewProduction(zap.Hooks(lumberjackZapHook))
    //if err != nil {
    //    fmt.Println(err)
    //    panic(err)
    //}
    //if err != nil {
    //    log.Fatalln("new log err:", err.Error())
    //}
    //Logger = logger.Sugar()

    return
}

func getLogWriter() zapcore.WriteSyncer {
    syncer := []zapcore.WriteSyncer{}
    // init log syncer
    var lumberJackLogger = &lumberjack.Logger{
        Filename:   config.LogFilePath + "app.log",
        MaxSize:    config.LogSize,    // megabytes
        MaxBackups: config.LogBackups, // number of log files
        MaxAge:     config.LogAge,     // days
    }
    syncer = append(syncer, zapcore.AddSync(lumberJackLogger))
    // init std syncer
    if config.LogToStd {
        w := os.Stdout
        syncer = append(syncer, zapcore.AddSync(w))
    }

    return zapcore.NewMultiWriteSyncer(syncer...)
}

func getEncoder() zapcore.Encoder {
    encoderConfig := zap.NewProductionEncoderConfig()
    encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
    encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
    if config.LogEncoding == "json" {
        return zapcore.NewJSONEncoder(encoderConfig)
    }
    return zapcore.NewConsoleEncoder(encoderConfig)
}
