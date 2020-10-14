package cache

import (
    "fmt"
    "sync"
    "time"

    "log"
)

// Table is a cache with sync.Map
type Table struct {
    sync.RWMutex

    items sync.Map

    // Timer responsible for triggering cleanup.
    cleanupTimer *time.Timer
    // Current timer duration.
    cleanupInterval time.Duration
}

// Item is the minimum data unit in cacheTable
type cacheItem struct {
    sync.RWMutex

    // The item's data
    data interface{}
    // How long will the item live in the cache when not being accessed
    lifeSpan time.Duration

    // Last access timestamp. first create accessedOn = time.Now()
    accessedOn time.Time
}

// Construct cacheItem
func newCacheItem(data interface{}, lifeSpan time.Duration) *cacheItem {
    return &cacheItem{
        data:       data,
        lifeSpan:   lifeSpan,
        accessedOn: time.Now(),
    }
}

// Expiration check loop, triggered by a self-adjusting timer
func (table *Table) expirationCheck() {
    table.Lock()
    if table.cleanupTimer != nil {
        table.cleanupTimer.Stop()
    }
    if table.cleanupInterval > 0 {
        log.Println(fmt.Sprintf("Expiration check triggered after", table.cleanupInterval))
    } else {
        log.Println("Expiration check init")
    }
    table.Unlock()

    now := time.Now()
    smallestDuration := 0 * time.Second

    table.items.Range(func(k, v interface{}) bool {
        item := v.(*cacheItem)
        lifeSpan := item.lifeSpan
        accessedOn := item.accessedOn
        if lifeSpan == 0 {
            return true
        }

        if now.Sub(accessedOn) >= lifeSpan {
            // Item has expired its lifespan.
            table.items.Delete(k)
        } else {
            // Find the item chronologically closest to its end-of-lifespan.
            if smallestDuration == 0 || lifeSpan-now.Sub(accessedOn) < smallestDuration {
                smallestDuration = lifeSpan - now.Sub(accessedOn)
            }
        }
        return true
    })

    // Setup the interval for the next cleanup run.
    table.Lock()
    table.cleanupInterval = smallestDuration
    if smallestDuration > 0 {
        table.cleanupTimer = time.AfterFunc(smallestDuration, func() {
            go table.expirationCheck()
        })
    }
    table.Unlock()
}

// Add item to Table
func (table *Table) Add(key string, data interface{}, expire time.Duration) {
    item := newCacheItem(data, expire)
    table.items.Store(key, item)

    table.RLock()
    expDur := table.cleanupInterval
    table.RUnlock()

    // If we haven't set up any expiration check timer or found a item expire soon.
    if item.lifeSpan > 0 && (expDur == 0 || item.lifeSpan < expDur) {
        table.expirationCheck()
    }
}

// Get data from Table by specific key
func (table *Table) Get(key string) (interface{}, bool) {
    val, ok := table.items.Load(key)
    if ok {
        item := val.(*cacheItem)

        // update item survival time
        item.Lock()
        item.accessedOn = time.Now()
        data := item.data
        item.Unlock()

        return data, ok
    }
    return nil, ok
}

// Get all key-value from table
func (table *Table) Range() map[string]interface{} {
    res := make(map[string]interface{})

    table.items.Range(func(key, value interface{}) bool {
        item := value.(*cacheItem)
        item.Lock()
        data := item.data
        item.Unlock()
        res[key.(string)] = data
        return true
    })

    return res
}

// Remove data from Table by a specific key
// don`t need to update cleanupInterval
func (table *Table) Delete(key string) {
    table.items.Delete(key)
}

// Clean table
func (table *Table) Clean() {
    table.items = sync.Map{}
    table.cleanupTimer = nil
    table.cleanupInterval = 0
}
