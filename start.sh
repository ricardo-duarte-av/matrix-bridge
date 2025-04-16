#!/bin/bash
while true
do
go run main.go processed_events_cache.go database_functions.go handlers.go
sleep 5
done
