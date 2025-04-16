package main

import (
    "time"
    "database/sql"
    _ "github.com/mattn/go-sqlite3"
    "log"
    "maunium.net/go/mautrix/id"
    "fmt"
)



// Database functions
func storeMessageMapping(db *sql.DB, mapping MessageMapping) error {
    _, err := db.Exec(`
        INSERT INTO message_mappings (
            source_event_id,
            target_event_id,
            source_room_id,
            target_room_id,
            created_at
        ) VALUES (?, ?, ?, ?, ?)`,
        mapping.SourceEventID,
        mapping.TargetEventID,
        mapping.SourceRoomID,
        mapping.TargetRoomID,
        mapping.CreatedAt)

    if err != nil {
        log.Printf("Error storing message mapping: %v", err)
    } else {
        log.Printf("Stored message mapping: %+v", mapping)
    }


    return err
}



func getMappedEventID(db *sql.DB, sourceEventID id.EventID, sourceRoom id.RoomID, targetRoom id.RoomID) (string, error) {
    var targetEventID string
    err := db.QueryRow(`
        SELECT target_event_id
        FROM message_mappings
        WHERE source_event_id = ?
        AND source_room_id = ?
        AND target_room_id = ?`,
        sourceEventID.String(),
        sourceRoom.String(),
        targetRoom.String()).Scan(&targetEventID)

    if err == sql.ErrNoRows {
        log.Printf("No mapping found for sourceEventID: %s, sourceRoom: %s, targetRoom: %s", sourceEventID, sourceRoom, targetRoom)
        return "", nil
    } else if err != nil {
        log.Printf("Error retrieving mapping for sourceEventID: %s, sourceRoom: %s, targetRoom: %s. Error: %v", sourceEventID, sourceRoom, targetRoom, err)
    } else {
        log.Printf("Retrieved mapping: sourceEventID=%s, targetEventID=%s, sourceRoom=%s, targetRoom=%s", sourceEventID, targetEventID, sourceRoom, targetRoom)
    }

    return targetEventID, err
}


func createTables(db *sql.DB) error {
    // Check if the message_mappings table already exists
    row := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='message_mappings'`)
    var tableName string
    err := row.Scan(&tableName)
    if err == nil && tableName == "message_mappings" {
        log.Printf("Table 'message_mappings' already exists, skipping creation.")
    } else {
        // Create message_mappings table if it does not exist
        _, err = db.Exec(`
            CREATE TABLE IF NOT EXISTS message_mappings (
                source_event_id TEXT NOT NULL,
                target_event_id TEXT NOT NULL,
                source_room_id TEXT NOT NULL,
                target_room_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (source_event_id, source_room_id, target_room_id),
                UNIQUE (source_event_id, source_room_id),
                UNIQUE (target_event_id, target_room_id)
            )`)
        if err != nil {
            return fmt.Errorf("failed to create message_mappings table: %v", err)
        }
        log.Printf("Table 'message_mappings' created successfully.")
    }

    // Create sync_tokens table
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS sync_tokens (
            server_name TEXT PRIMARY KEY,
            next_batch TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )`)
    if err != nil {
        return fmt.Errorf("failed to create sync_tokens table: %v", err)
    }

    return nil
}




func getSyncToken(db *sql.DB, serverName string) (string, error) {
    var token string
    err := db.QueryRow(`
        SELECT next_batch FROM sync_tokens
        WHERE server_name = ?
    `, serverName).Scan(&token)
    if err == sql.ErrNoRows {
        return "", nil // Return empty string for first sync
    }
    return token, err
}


func storeSyncToken(db *sql.DB, serverName, token string) error {
    // Create table if it doesn't exist
    _, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS sync_tokens (
            server_name TEXT PRIMARY KEY,
            next_batch TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
    `)
    if err != nil {
        return fmt.Errorf("failed to create sync_tokens table: %v", err)
    }

    // Store/update the token
    _, err = db.Exec(`
        INSERT INTO sync_tokens (server_name, next_batch, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(server_name) DO UPDATE SET
            next_batch = excluded.next_batch,
            updated_at = excluded.updated_at
    `, serverName, token, time.Now().UTC())

    if err != nil {
        return fmt.Errorf("failed to store sync token: %v", err)
    }

    return nil
}



func getMessageMapping(db *sql.DB, sourceEventID id.EventID) (MessageMapping, error) {
    var mapping MessageMapping

    // Query the database for the message mapping
    query := `
        SELECT source_event_id, target_event_id, source_room_id, target_room_id, created_at
        FROM message_mappings
        WHERE source_event_id = ?
    `
    err := db.QueryRow(query, sourceEventID).Scan(
        &mapping.SourceEventID,
        &mapping.TargetEventID,
        &mapping.SourceRoomID,
        &mapping.TargetRoomID,
        &mapping.CreatedAt,
    )

    if err != nil {
        // Handle cases where no mapping is found
        if err == sql.ErrNoRows {
            return MessageMapping{}, fmt.Errorf("no message mapping found for sourceEventID: %s", sourceEventID)
        }
        // Handle other database errors
        return MessageMapping{}, fmt.Errorf("failed to get message mapping: %v", err)
    }

    return mapping, nil
}


func deleteMessageMapping(db *sql.DB, sourceEventID id.EventID) error {
    _, err := db.Exec(`
        DELETE FROM message_mappings
        WHERE source_event_id = ?
    `, sourceEventID)

    if err != nil {
        return fmt.Errorf("failed to delete message mapping: %v", err)
    }
    return nil
}
