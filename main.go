package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "os"
    "os/signal"
    "strings"
    "syscall"
    "time"
    "sync"  // Add this import
    _ "github.com/mattn/go-sqlite3"
    "gopkg.in/yaml.v3"
    "maunium.net/go/mautrix"
    "maunium.net/go/mautrix/event"
    "maunium.net/go/mautrix/id"
    "html"
)

// Add at the top of the file with other global variables
var appStartTime time.Time


// Config represents the structure of the configuration file.
type Config struct {
    Servers []Server `yaml:"servers"`
    Links   []Link   `yaml:"room_links"`
}

// Server represents a Matrix server configuration.
type Server struct {
    Name     string `yaml:"name"`
    URL      string `yaml:"url"`
    User     string `yaml:"user"`
    Password string `yaml:"password"`
}

// Link represents a room link configuration.
type Link struct {
    From          string `yaml:"from"`
    FromUser      string `yaml:"from_user"`
    FromNickname  string `yaml:"from_nickname,omitempty"`
    FromAvatar    string `yaml:"from_avatar,omitempty"`
    To            string `yaml:"to"`
    ToUser        string `yaml:"to_user"`
    ToNickname    string `yaml:"to_nickname,omitempty"`
    ToAvatar      string `yaml:"to_avatar,omitempty"`
}


// Message mapping struct
type MessageMapping struct {
    SourceEventID string
    TargetEventID string
    SourceRoomID  string
    TargetRoomID  string
    CreatedAt     time.Time
}


// PerMessageProfile structure
type PerMessageProfile struct {
    ID          string `json:"id"`
    DisplayName string `json:"displayname"`
    AvatarURL   string `json:"avatar_url"`
}

// syncError type for handling sync errors
type syncError struct {
    serverName string
    err       error
}

type ClientSync struct {
    client *mautrix.Client
    name   string
    mutex  sync.Mutex
}


// Room nicname and avatar functions
func setRoomProfile(ctx context.Context, client *mautrix.Client, roomID id.RoomID, nickname, avatar string) error {
        // Fetch the current membership event for the user in this room
        userID := client.UserID
        var membershipEvent event.MemberEventContent
        err := client.StateEvent(ctx, roomID, event.StateMember, userID.String(), &membershipEvent)
        if err != nil {
                return fmt.Errorf("failed to fetch membership event for room %s: %w", roomID, err)
        }

        // Check if nickname needs to be updated
        needsUpdate := false
        if nickname != "" && membershipEvent.Displayname != nickname {
                log.Printf("Updating nickname in room %s from '%s' to '%s'", roomID, membershipEvent.Displayname, nickname)
                membershipEvent.Displayname = nickname
                needsUpdate = true
        } else if nickname != "" {
                log.Printf("Nickname in room %s is already '%s', no update needed", roomID, nickname)
        }

        // Check if avatar needs to be updated
        if avatar != "" {
                avatarURI, err := id.ParseContentURI(avatar) // Parse the avatar string into id.ContentURI
                if err != nil {
                        return fmt.Errorf("failed to parse avatar URI '%s': %w", avatar, err)
                }
                avatarString := avatarURI.CUString() // Convert id.ContentURI to id.ContentURIString
                if membershipEvent.AvatarURL != avatarString {
                        log.Printf("Updating avatar in room %s from '%s' to '%s'", roomID, membershipEvent.AvatarURL, avatarString)
                        membershipEvent.AvatarURL = avatarString
                        needsUpdate = true
                } else {
                        log.Printf("Avatar in room %s is already '%s', no update needed", roomID, avatarString)
                }
        }

        // If no changes are needed, skip the update
        if !needsUpdate {
                log.Printf("No changes needed for room %s, skipping update", roomID)
                return nil
        }

        // Send the updated membership event
        resp, err := client.SendStateEvent(ctx, roomID, event.StateMember, userID.String(), &membershipEvent)
        if err != nil {
                return fmt.Errorf("failed to send updated membership event for room %s: %w", roomID, err)
        }
        log.Printf("Successfully updated profile for room %s (nickname: '%s', avatar: '%s'), response: %+v", roomID, nickname, avatar, resp)
        return nil
}




func processRoomLinks(ctx context.Context, clients map[string]*mautrix.Client, links []Link) {
    for _, link := range links {
        fromRoom := id.RoomID(link.From)
        toRoom := id.RoomID(link.To)

        // Get the clients for the respective users
        fromClient := getClientForUser(link.FromUser, clients)
        toClient := getClientForUser(link.ToUser, clients)

        // Set profile for the "from" user in the "from" room
        if fromClient != nil {
            if err := setRoomProfile(ctx, fromClient, fromRoom, link.FromNickname, link.FromAvatar); err != nil {
                log.Printf("Error setting profile for 'from' user in room %s: %v", fromRoom, err)
            }
        }

        // Set profile for the "to" user in the "to" room
        if toClient != nil {
            if err := setRoomProfile(ctx, toClient, toRoom, link.ToNickname, link.ToAvatar); err != nil {
                log.Printf("Error setting profile for 'to' user in room %s: %v", toRoom, err)
            }
        }
    }
}




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

//
func createTables(db *sql.DB) error {
    // Drop existing tables if they exist
    _, err := db.Exec(`DROP TABLE IF EXISTS message_mappings`)
    if err != nil {
        return fmt.Errorf("failed to drop message_mappings table: %v", err)
    }

    // Create message_mappings table with correct schema
    _, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS message_mappings (
            source_event_id TEXT NOT NULL,
            target_event_id TEXT NOT NULL,
            source_room_id TEXT NOT NULL,
            target_room_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_event_id, source_room_id, target_room_id)
        )`)
    if err != nil {
        return fmt.Errorf("failed to create message_mappings table: %v", err)
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



func isRoomJoined(ctx context.Context, client *mautrix.Client, roomID id.RoomID) (bool, error) {
    // Attempt to fetch the joined members of the room
    joinedMembers, err := client.JoinedMembers(ctx, roomID)
    if err != nil {
        // If the API returns an error, assume the bot is not joined
        if strings.Contains(err.Error(), "M_FORBIDDEN") {
            // Forbidden error indicates the bot is not joined
            return false, nil
        }
        return false, fmt.Errorf("failed to fetch joined members for room %s: %w", roomID.String(), err)
    }

    // Check if the bot's user ID is among the joined members
    for member := range joinedMembers.Joined {
        if member == client.UserID {
            return true, nil
        }
    }

    return false, nil
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

func getClientForUser(user string, clients map[string]*mautrix.Client) *mautrix.Client {
    for _, client := range clients {
        if client.UserID.String() == user {
            return client
        }
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



// Helper function to update reply fallback in formatted body
func updateReplyFallback(formattedBody, originalSender, originalBody string) string {
    log.Printf("updateReplyFallback called with:")
    log.Printf("formattedBody: %s", formattedBody)
    log.Printf("originalSender: %s", originalSender)
    log.Printf("originalBody: %s", originalBody)

    // Use the HTMLReplyFallbackRegex to find the <mx-reply> block
    replyMatch := event.HTMLReplyFallbackRegex.FindStringIndex(formattedBody)
    if replyMatch == nil {
        log.Printf("No <mx-reply> block found in formattedBody.")
        return formattedBody
    }

    // Extract the actual message content (after </mx-reply>)
    replyEnd := replyMatch[1] // End of the match
    messageContent := formattedBody[replyEnd:]
    log.Printf("Extracted messageContent: %s", messageContent)

    // Create new reply fallback
    replyFallback := fmt.Sprintf(
        "<mx-reply><blockquote><a>In reply to</a> <a>%s</a><br>%s</blockquote></mx-reply>",
        html.EscapeString(originalSender),
        html.EscapeString(originalBody),
    )
    log.Printf("Constructed replyFallback: %s", replyFallback)

    finalBody := replyFallback + messageContent
    log.Printf("Final updated formattedBody: %s", finalBody)

    return finalBody
}



// Helper function to get user profile
func getUserProfile(ctx context.Context, client *mautrix.Client, userID id.UserID) (*PerMessageProfile, error) {
    profile, err := client.GetProfile(ctx, userID)
    if err != nil {
        return nil, fmt.Errorf("failed to get user profile: %v", err)
    }

    return &PerMessageProfile{
        ID:          userID.String(),
        DisplayName: profile.DisplayName,
        AvatarURL:   profile.AvatarURL.String(),
    }, nil
}


//
// MSG Handlers
//

func handleMessageEvent(ctx context.Context, evt *event.Event, client *mautrix.Client,
        roomLinks map[id.RoomID]id.RoomID, roomClients map[id.RoomID]*mautrix.Client, db *sql.DB) {

        // Log the incoming event for debugging purposes
        //log.Printf("Processing message event: %v", evt)

        // Step 1: Check if the event's room is linked to another room
        if targetRoom, ok := roomLinks[evt.RoomID]; ok {

                log.Printf("==================================================================================================================================================================================================")
                // Step 2: Check if the event is from before the bot started
                if !isEventAfterStartTime(evt) {
                        //log.Printf("Event is from before bot start time: %v", evt.ID)

                        // Add the event to the mapping table but do not bridge it
                        mapping := MessageMapping{
                                SourceEventID: evt.ID.String(),
                                //TargetEventID: sentEvent.EventID.String(),
                                SourceRoomID:  evt.RoomID.String(),
                                TargetRoomID:  targetRoom.String(),
                                CreatedAt:     time.Now(),

                        }
                        err := storeMessageMapping(db, mapping)
                        if err != nil {
                                log.Printf("ANCIENT: Error storing message mapping for pre-start event: %v", err)
                        } else {
                                log.Printf("ANCIENT: Stored pre-start event mapping successfully: %+v", mapping)
                        }
                        return
                }

                log.Printf("Found target room for event: %s -> %s", evt.RoomID, targetRoom)

                // Get the target client for sending messages to the linked room
                targetClient := roomClients[targetRoom]

                // Step 3: Ensure the event is not sent by the bot itself
                if evt.Sender != client.UserID {
                        content := evt.Content.AsMessage()
                        log.Printf("Processing content: %v", content)

                        // Step 4: Handle threads and replies
                        if content.RelatesTo != nil {
                                log.Printf("Detected related message: relates_to=%v", content.RelatesTo)

                                // Step 4.1: Map the related event ID
                                relatedEventID := content.RelatesTo.EventID
                                bridgedEvent, err := getMessageMapping(db, id.EventID(relatedEventID))
                                if err != nil {
                                        log.Printf("Error fetching mapping for related event %s: %v", relatedEventID, err)
                                        return
                                }

                                if bridgedEvent.TargetEventID == "" {
                                        log.Printf("No mapping found for related event %s in destination room.", relatedEventID)
                                        return
                                }

                                // Step 4.2: Update the related event ID to the mapped event ID in the destination room
                                content.RelatesTo.EventID = id.EventID(bridgedEvent.TargetEventID)
                                if content.RelatesTo.InReplyTo != nil {
                                        content.RelatesTo.InReplyTo.EventID = id.EventID(bridgedEvent.TargetEventID)
                                }
                                log.Printf("Updated related event ID to %s", bridgedEvent.TargetEventID)
                        }

                        // Step 5: Serialize the message content to JSON
                        contentBytes, err := json.Marshal(content)
                        if err != nil {
                                log.Printf("Error serializing message content: %v", err)
                                return
                        }

                        // Step 6: Deserialize the JSON into a map
                        var contentMap map[string]interface{}
                        if err := json.Unmarshal(contentBytes, &contentMap); err != nil {
                                log.Printf("Error deserializing message content to map: %v", err)
                                return
                        }

                        // Step 7: Fetch the sender's profile (avatar URL and display name)
                        displayName, avatarURL, err := fetchSenderProfile(ctx, client, evt.Sender)
                        if err != nil {
                                log.Printf("Error fetching sender profile for %s: %v", evt.Sender, err)
                                displayName = "Matrix-Bridge"
                                avatarURL = "mxc://aguiarvieira.pt/4c65be941db57740194d5b323ef7e81cafafa7141911847812484575232"
                        }
                        log.Printf("Fetched sender profile: displayName=%s, avatarURL=%s", displayName, avatarURL)

                        // Step 8: Add the per-message profile metadata
                        contentMap["com.beeper.per_message_profile"] = map[string]interface{}{
                                "avatar_url":  avatarURL,
                                "displayname": displayName,
                                "id":          evt.Sender.String(),
                        }
                        log.Printf("Added per-message profile to content: %+v", contentMap)

                        // Step 9: Forward the processed message to the target room
                        log.Printf("Forwarding message from %s to %s.", evt.RoomID, targetRoom)
                        sentEvent, err := targetClient.SendMessageEvent(ctx, targetRoom, event.EventMessage, contentMap)
                        if err != nil {
                                log.Printf("Error forwarding message: %v", err)
                                return
                        }
                        log.Printf("Message forwarded successfully. SourceEventID=%s, TargetEventID=%s", evt.ID, sentEvent.EventID)

                        // Step 10: Store the event mapping for the bridged message
                        mapping := MessageMapping{
                                SourceEventID: evt.ID.String(),
                                TargetEventID: sentEvent.EventID.String(),
                                SourceRoomID:  evt.RoomID.String(),
                                TargetRoomID:  targetRoom.String(),
                                CreatedAt:     time.Now(),
                        }
                        err = storeMessageMapping(db, mapping)
                        if err != nil {
                                log.Printf("Error storing message mapping: %v", err)
                        } else {
                                log.Printf("Message mapping stored successfully: %+v", mapping)
                        }
                }
        } //else {
        //      log.Printf("No target room linked for event in room %s.", evt.RoomID)
        //}
}















// fetchSenderProfile fetches the display name and avatar URL of the sender
func fetchSenderProfile(ctx context.Context, client *mautrix.Client, sender id.UserID) (string, string, error) {
    profile, err := client.GetProfile(ctx, sender)
    if err != nil {
        return "", "", fmt.Errorf("failed to fetch profile for sender %s: %w", sender, err)
    }
    return profile.DisplayName, profile.AvatarURL.String(), nil
}







func handleReactionEvent(ctx context.Context, evt *event.Event, client *mautrix.Client,
    roomLinks map[id.RoomID]id.RoomID, roomClients map[id.RoomID]*mautrix.Client, db *sql.DB) {

    // Skip reactions from before app start
    if !isEventAfterStartTime(evt) {
        return
    }


    if targetRoom, ok := roomLinks[evt.RoomID]; ok {
        targetClient := roomClients[targetRoom]

        if evt.Sender != client.UserID {
            content := evt.Content.AsReaction()

            // Get the mapped event ID for the reacted message
            targetEventID, err := getMappedEventID(db, content.RelatesTo.EventID, evt.RoomID, targetRoom)
            if err != nil {
                log.Printf("Error getting mapped event ID: %v", err)
                return
            }
            if targetEventID == "" {
                log.Printf("Could not find mapped event ID for reaction")
                return
            }

            // Check if this is a reaction removal (when m.relates_to.key is empty)
            isRemoval := content.RelatesTo.Key == ""

            if isRemoval {
                // Handle reaction removal
                // First, get the original reaction event mapping
                originalReactionEventID, err := getMappedEventID(db, evt.ID, evt.RoomID, targetRoom)
                if err != nil {
                    log.Printf("Error getting mapped reaction event ID: %v", err)
                    return
                }
                if originalReactionEventID != "" {
                    // If we found the original reaction, redact it
                    _, err = targetClient.RedactEvent(ctx, targetRoom, id.EventID(originalReactionEventID))
                    if err != nil {
                        log.Printf("Error removing reaction: %v", err)
                    }
                    // Delete the reaction mapping
                    err = deleteMessageMapping(db, evt.ID)
                    if err != nil {
                        log.Printf("Error deleting reaction mapping: %v", err)
                    }
                }
            } else {
                // Create reaction content
                newContent := event.ReactionEventContent{
                    RelatesTo: event.RelatesTo{
                        EventID: id.EventID(targetEventID),
                        Type:    content.RelatesTo.Type,
                        Key:     content.RelatesTo.Key,
                    },
                }

                // Send the reaction
                resp, err := targetClient.SendMessageEvent(ctx, targetRoom, event.EventReaction, newContent)
                if err != nil {
                    log.Printf("Error forwarding reaction: %v", err)
                    return
                }

                // Store the reaction mapping
                err = storeMessageMapping(db, MessageMapping{
                    SourceEventID: evt.ID.String(),
                    TargetEventID: resp.EventID.String(),
                    SourceRoomID:  evt.RoomID.String(),
                    TargetRoomID:  targetRoom.String(),
                    CreatedAt:     time.Now().UTC(),
                })
                if err != nil {
                    log.Printf("Error storing reaction mapping: %v", err)
                }
            }
        }
    }
}



func handleRedactionEvent(ctx context.Context, evt *event.Event, client *mautrix.Client,
    roomLinks map[id.RoomID]id.RoomID, roomClients map[id.RoomID]*mautrix.Client, db *sql.DB) {

    // Skip redactions from before app start
    if !isEventAfterStartTime(evt) {
        return
    }


    if targetRoom, ok := roomLinks[evt.RoomID]; ok {
        targetClient := roomClients[targetRoom]

        if evt.Sender != client.UserID {
            // Get the mapped event ID for the redacted message
            targetEventID, err := getMappedEventID(db, evt.Redacts, evt.RoomID, targetRoom)
            if err != nil {
                log.Printf("Error getting mapped event ID: %v", err)
                return
            }
            if targetEventID == "" {
                log.Printf("Could not find mapped event ID for redaction")
                return
            }

            _, err = targetClient.RedactEvent(ctx, targetRoom, id.EventID(targetEventID))
            if err != nil {
                log.Printf("Error forwarding redaction: %v", err)
            }
        }
    }
}






// Update your main function's event handler registrations to:
func registerEventHandlers(syncer *mautrix.DefaultSyncer, client *mautrix.Client,
    roomLinks map[id.RoomID]id.RoomID, roomClients map[id.RoomID]*mautrix.Client, db *sql.DB) {

    // Register event handlers for each client
    syncer.OnEventType(event.EventMessage, func(ctx context.Context, evt *event.Event) {
        handleMessageEvent(ctx, evt, client, roomLinks, roomClients, db)
    })

    // Handle reaction events
    syncer.OnEventType(event.EventReaction, func(ctx context.Context, evt *event.Event) {
        handleReactionEvent(ctx, evt, client, roomLinks, roomClients, db)
    })

    // Handle redaction events
    syncer.OnEventType(event.EventRedaction, func(ctx context.Context, evt *event.Event) {
        handleRedactionEvent(ctx, evt, client, roomLinks, roomClients, db)
    })
}





// loadConfig loads the configuration from a YAML file.
func loadConfig(filename string) (*Config, error) {
    config := &Config{}
    file, err := os.Open(filename)
    if err != nil {
        return nil, err
    }
    defer file.Close()
    decoder := yaml.NewDecoder(file)
    err = decoder.Decode(config)
    if err != nil {
        return nil, err
    }
    return config, nil
}

// connectToServer connects to a Matrix server using the provided URL, username, and password.
func connectToServer(ctx context.Context, url, username, password string) (*mautrix.Client, error) {
    client, err := mautrix.NewClient(url, "", "")
    if err != nil {
        return nil, err
    }

    user := username
    if strings.HasPrefix(user, "@") {
        parts := strings.Split(user, ":")
        if len(parts) >= 1 {
            user = strings.TrimPrefix(parts[0], "@")
        }
    }

    resp, err := client.Login(ctx, &mautrix.ReqLogin{
        Type: "m.login.password",
        Identifier: mautrix.UserIdentifier{
            Type: "m.id.user",
            User: user,
        },
        Password: password,
    })
    if err != nil {
        return nil, err
    }

    client.AccessToken = resp.AccessToken
    client.UserID = resp.UserID
    log.Printf("Successfully logged in as %s", client.UserID)

    return client, nil
}

func isEventAfterStartTime(evt *event.Event) bool {
    // Convert milliseconds to time.Time
    eventTime := time.Unix(evt.Timestamp/1000, 0).UTC()
    return eventTime.After(appStartTime)
}



//
// MAIN
//



func main() {
    // Set application start time in UTC
    appStartTime = time.Now().UTC()

    log.Printf("Matrix Room Bridge starting at %s", appStartTime.Format("2006-01-02 15:04:05"))
    log.Printf("Running as user: ricardo-duarte-av")

    // Load configuration
    config, err := loadConfig("config.yaml")
    if err != nil {
        log.Fatalf("Error loading configuration: %v", err)
    }
    log.Printf("Loaded configuration with %d servers and %d room links",
        len(config.Servers), len(config.Links))

    // Initialize SQLite database
    db, err := sql.Open("sqlite3", "bridge.db")
    if err != nil {
        log.Fatalf("Error opening database: %v", err)
    }
    defer db.Close()

    // Create message mappings table
    err = createTables(db)
    if err != nil {
        log.Fatalf("Error creating tables: %v", err)
    }

    // Create context with cancellation
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Channel for sync errors
    errChan := make(chan syncError)

    // Connect to all servers
    clients := make(map[string]*mautrix.Client)
    for _, server := range config.Servers {
        log.Printf("Connecting to server %s at %s...",
            server.Name, server.URL)

        client, err := connectToServer(ctx, server.URL, server.User, server.Password)
        if err != nil {
            log.Fatalf("Failed to connect to server %s: %v", server.Name, err)
        }
        clients[server.Name] = client

        log.Printf("Successfully connected to server %s as %s",
            server.Name, client.UserID)
    }

    // Create mappings for room links
    roomLinks := make(map[id.RoomID]id.RoomID)     // from -> to
    roomClients := make(map[id.RoomID]*mautrix.Client) // room -> client

        // Process room links and build mappings
        for i, link := range config.Links {
                fromRoom := id.RoomID(link.From)
                toRoom := id.RoomID(link.To)

                log.Printf("Processing link %d: %s (%s) -> %s (%s)",
                        i+1, fromRoom, link.FromUser, toRoom, link.ToUser)

                // Get the clients for the specified users
                fromClient := getClientForUser(link.FromUser, clients)
                toClient := getClientForUser(link.ToUser, clients)

                if fromClient == nil || toClient == nil {
                        log.Printf("Error: Could not find client for one of the users in link %d", i+1)
                        continue
                }

                // Store mappings
                roomLinks[fromRoom] = toRoom
                roomClients[fromRoom] = fromClient
                roomClients[toRoom] = toClient

                // Join rooms
                if joined, err := isRoomJoined(ctx, fromClient, fromRoom); err != nil {
                        log.Printf("Warning: Failed to check room %s: %v", fromRoom, err)
                } else if !joined {
                        if _, err := fromClient.JoinRoom(ctx, fromRoom.String(), nil); err != nil {
                                log.Printf("Warning: Failed to join room %s: %v", fromRoom, err)
                        } else {
                                log.Printf("Successfully joined room %s", fromRoom)
                        }
                }

                if joined, err := isRoomJoined(ctx, toClient, toRoom); err != nil {
                        log.Printf("Warning: Failed to check room %s: %v", toRoom, err)
                } else if !joined {
                        if _, err := toClient.JoinRoom(ctx, toRoom.String(), nil); err != nil {
                                log.Printf("Warning: Failed to join room %s: %v", toRoom, err)
                        } else {
                                log.Printf("Successfully joined room %s", toRoom)
                        }
                }

                // After joining rooms, set nicknames and avatars
                log.Println("Setting room profiles (nicknames and avatars)...")
                processRoomLinks(ctx, clients, config.Links)


        }

    // Start sync for all clients
    log.Println("Starting sync for all clients")

    for serverName, client := range clients {
        name := serverName
        c := client

        // Get the last sync token for this server
        token, err := getSyncToken(db, name)
        if err != nil {
            log.Printf("Error getting sync token: %v - starting fresh", err)
        }

        syncer := c.Syncer.(*mautrix.DefaultSyncer)

        // Register event handlers
        registerEventHandlers(syncer, c, roomLinks, roomClients, db)


        // Start sync loop
        go func() {
            for {
                log.Printf("Starting sync loop for %s", name)

                if token == "" {
                    // First sync - create and use a filter
                    filter := &mautrix.Filter{
                        Room: &mautrix.RoomFilter{
                            Timeline: &mautrix.FilterPart{
                                Limit: 50,
                                Types: []event.Type{
                                    event.EventMessage,
                                    event.EventReaction,
                                    event.EventRedaction,
                                },
                            },
                            State: &mautrix.FilterPart{
                                LazyLoadMembers: true,
                            },
                        },
                    }

                    filterID, err := c.CreateFilter(ctx, filter)
                    if err != nil {
                        log.Printf("Error creating initial filter: %v", err)
                    } else {
                        log.Printf("Created filter %s for client %s", filterID.FilterID, name)
                        c.Store.SaveFilterID(ctx, c.UserID, filterID.FilterID)
                    }
                } else {
                    log.Printf("Resuming sync for %s with token %s", name, token)
                }

                syncErr := c.SyncWithContext(ctx)
                if syncErr != nil {
                    errChan <- syncError{
                        serverName: name,
                        err:       syncErr,
                    }
                    log.Printf("Sync error for %s: %v - retrying in 5 seconds",
                        name, syncErr)
                    time.Sleep(5 * time.Second)

                    token, err = getSyncToken(db, name)
                    if err != nil {
                        log.Printf("Error getting sync token after error: %v", err)
                    }
                    continue
                }

                if c.Store != nil {
                    nextBatch, err := c.Store.LoadNextBatch(ctx, c.UserID)
                    if err != nil {
                        log.Printf("Error loading next batch: %v", err)
                    } else if nextBatch != "" && nextBatch != token {
                        token = nextBatch
                        if err := storeSyncToken(db, name, token); err != nil {
                            log.Printf("Error storing sync token: %v", err)
                        }
                    }
                }
            }
        }()
    }

    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

    select {
    case err := <-errChan:
        log.Printf("Error in sync loop for %s: %v",
            err.serverName, err.err)
    case sig := <-sigChan:
        log.Printf("Received signal %v, shutting down...", sig)
    }

    cancel()
}
