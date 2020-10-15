package tools

var defaultPanicHandler = func(errStr interface{}) {

}

type PanicHandler func(errStr interface{})

func WithRecover(fn func(), panicHandlers ...PanicHandler) {
    defer func() {
        var handlers []PanicHandler
        if panicHandlers != nil && len(panicHandlers) > 0 {
            handlers = panicHandlers
        } else {
            handlers = []PanicHandler{defaultPanicHandler}
        }
        if e := recover(); e != nil {
            for _, h := range handlers {
                h(e)
            }
        }
    }()
    // call
    fn()
}
