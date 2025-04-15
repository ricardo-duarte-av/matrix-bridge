package main

import (
    "sync"
    "time"
)

// ProcessedEventsCache is a thread-safe cache for storing recently processed event IDs
type ProcessedEventsCache struct {
    cache   map[string]time.Time
    maxSize int
    mu      sync.Mutex
}

// NewProcessedEventsCache creates a new cache with a specific max size
func NewProcessedEventsCache(maxSize int) *ProcessedEventsCache {
    return &ProcessedEventsCache{
        cache:   make(map[string]time.Time),
        maxSize: maxSize,
    }
}

// Add adds an event ID to the cache and evicts the oldest if necessary
func (c *ProcessedEventsCache) Add(eventID string) {
    c.mu.Lock()
    defer c.mu.Unlock()

    // Add the event ID with the current timestamp
    c.cache[eventID] = time.Now()

    // Evict the oldest event if the cache exceeds the max size
    if len(c.cache) > c.maxSize {
        oldestID := ""
        oldestTime := time.Now()
        for id, t := range c.cache {
            if t.Before(oldestTime) {
                oldestID = id
                oldestTime = t
            }
        }
        if oldestID != "" {
            delete(c.cache, oldestID)
        }
    }
}

// Contains checks if an event ID is already in the cache
func (c *ProcessedEventsCache) Contains(eventID string) bool {
    c.mu.Lock()
    defer c.mu.Unlock()

    _, exists := c.cache[eventID]
    return exists
}
