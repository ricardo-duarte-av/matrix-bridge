package main

import (
    "context"
    "database/sql"
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
    SourceEventID  string
    TargetEventID  string
    SourceRoomID   string
    TargetRoomID   string
    CreatedAt      time.Time
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
        return "", nil
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
    err := db.QueryRow(`
        SELECT source_event_id, target_event_id, source_room_id, target_room_id, created_at
        FROM message_mappings
        WHERE source_event_id = ?
    `, sourceEventID).Scan(
        &mapping.SourceEventID,
        &mapping.TargetEventID,
        &mapping.SourceRoomID,
        &mapping.TargetRoomID,
        &mapping.CreatedAt,
    )

    if err != nil {
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
    // Find the reply fallback block (everything between first <mx-reply> tags)
    replyStart := strings.Index(formattedBody, "<mx-reply>")
    replyEnd := strings.Index(formattedBody, "</mx-reply>")

    if replyStart == -1 || replyEnd == -1 {
        return formattedBody
    }

    // Extract the actual message content (after </mx-reply>)
    messageContent := formattedBody[replyEnd+11:] // 11 is length of "</mx-reply>"

    // Create new reply fallback
    replyFallback := fmt.Sprintf(
        "<mx-reply><blockquote><a>In reply to</a> <a>%s</a><br>%s</blockquote></mx-reply>",
        html.EscapeString(originalSender),
        html.EscapeString(originalBody),
    )

    return replyFallback + messageContent
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

    // Skip messages from before app start
    if !isEventAfterStartTime(evt) {
        return
    }

    if targetRoom, ok := roomLinks[evt.RoomID]; ok {
        targetClient := roomClients[targetRoom]

        if evt.Sender != client.UserID {
            content := evt.Content.AsMessage()
            log.Printf("Processing message event in room %s", evt.RoomID)

            // Get sender's profile for per_message_profile
            profile, err := getUserProfile(ctx, client, evt.Sender)
            if err != nil {
                log.Printf("Error getting user profile: %v", err)
                // Continue anyway, as profile is optional
            }

            // Check if this is an edit
            if content.NewContent != nil && content.RelatesTo != nil &&
               content.RelatesTo.Type == event.RelReplace {

                // Get the original message mapping
                targetEventID, err := getMappedEventID(db, content.RelatesTo.EventID, evt.RoomID, targetRoom)
                if err != nil {
                    log.Printf("Error getting mapped event ID for edit: %v", err)
                    return
                }
                if targetEventID == "" {
                    log.Printf("Could not find mapped event ID for edit")
                    return
                }

                // Create edit content
                editContent := map[string]interface{}{
                    "msgtype": content.NewContent.MsgType,
                    "body":    content.NewContent.Body,
                    "m.relates_to": map[string]interface{}{
                        "rel_type": event.RelReplace,
                        "event_id": targetEventID,
                    },
                }

                if content.NewContent.FormattedBody != "" {
                    editContent["formatted_body"] = content.NewContent.FormattedBody
                    editContent["format"] = content.NewContent.Format
                }

                // Add profile information
                if profile != nil {
                    editContent["com.beeper.per_message_profile"] = profile
                }

                // Send the edit
                resp, err := targetClient.SendMessageEvent(ctx, targetRoom, event.EventMessage, editContent)
                if err != nil {
                    log.Printf("Error sending edit: %v", err)
                    return
                }

                // Store mapping for the edit
                err = storeMessageMapping(db, MessageMapping{
                    SourceEventID: evt.ID.String(),
                    TargetEventID: resp.EventID.String(),
                    SourceRoomID:  evt.RoomID.String(),
                    TargetRoomID:  targetRoom.String(),
                    CreatedAt:     time.Now().UTC(),
                })
                if err != nil {
                    log.Printf("Error storing edit mapping: %v", err)
                }
                return
            }

            // Handle media messages
            if content.MsgType == event.MsgImage || content.MsgType == event.MsgVideo ||
               content.MsgType == event.MsgAudio || content.MsgType == event.MsgFile {

                // Download media from source
                mxc := content.URL
                if mxc == "" {
                    log.Printf("No URL found in media message")
                    return
                }

                // Parse MXC URL
                parsedMXC, err := id.ParseContentURI(string(mxc))
                if err != nil {
                    log.Printf("Error parsing MXC URL: %v", err)
                    return
                }

                // Download file
                data, err := client.DownloadBytes(ctx, parsedMXC)
                if err != nil {
                    log.Printf("Error downloading media: %v", err)
                    return
                }

                // Upload to target server
                resp, err := targetClient.UploadBytes(ctx, data, content.Info.MimeType)
                if err != nil {
                    log.Printf("Error uploading media: %v", err)
                    return
                }

                // Get sender's profile
                profile, err := getUserProfile(ctx, client, evt.Sender)
                if err != nil {
                    log.Printf("Error getting user profile: %v", err)
                    // Continue anyway as profile is optional
                }

                // Create content map preserving all original metadata
                contentMap := map[string]interface{}{
                    "body":    content.Body,
                    "msgtype": content.MsgType,
                    "url":     resp.ContentURI.CUString(),
                }

                // Preserve all original info fields
                if content.Info != nil {
                    info := make(map[string]interface{})

                    // Copy basic info fields
                    if content.Info.Height != 0 {
                        info["h"] = content.Info.Height
                    }
                    if content.Info.Width != 0 {
                        info["w"] = content.Info.Width
                    }
                    if content.Info.Size != 0 {
                        info["size"] = content.Info.Size
                    }
                    if content.Info.MimeType != "" {
                        info["mimetype"] = content.Info.MimeType
                    }

                    // Copy any additional fields from raw content
                    if rawContent, ok := evt.Content.Raw["info"].(map[string]interface{}); ok {
                        for k, v := range rawContent {
                            if _, exists := info[k]; !exists {
                                info[k] = v
                            }
                        }
                    }

                    contentMap["info"] = info
                }

                // Add profile information
                if profile != nil {
                    contentMap["com.beeper.per_message_profile"] = profile
                }

                // Copy any m.mentions if present
                if mentions, ok := evt.Content.Raw["m.mentions"].(map[string]interface{}); ok {
                    contentMap["m.mentions"] = mentions
                }

                // Send the message
                resp2, err := targetClient.SendMessageEvent(ctx, targetRoom, event.EventMessage, contentMap)
                if err != nil {
                    log.Printf("Error forwarding media message: %v", err)
                    return
                }

                // Store the message mapping
                err = storeMessageMapping(db, MessageMapping{
                    SourceEventID: evt.ID.String(),
                    TargetEventID: resp2.EventID.String(),
                    SourceRoomID:  evt.RoomID.String(),
                    TargetRoomID:  targetRoom.String(),
                    CreatedAt:     time.Now().UTC(),
                })
                if err != nil {
                    log.Printf("Error storing message mapping: %v", err)
                }
                return
            }

            // Create base content map
            contentMap := map[string]interface{}{
                "msgtype": content.MsgType,
                "body":    content.Body,
            }

            // Add formatted body if present
            if content.FormattedBody != "" {
                contentMap["formatted_body"] = content.FormattedBody
                contentMap["format"] = content.Format
            }

            // Add profile information
            if profile != nil {
                contentMap["com.beeper.per_message_profile"] = profile
            }

            // Send the message
            resp, err := targetClient.SendMessageEvent(ctx, targetRoom, event.EventMessage, contentMap)
            if err != nil {
                log.Printf("Error forwarding message: %v", err)
            } else {
                log.Printf("Successfully forwarded message to %s with event ID %s", targetRoom, resp.EventID)
                // Store the message mapping
                mapping := MessageMapping{
                    SourceEventID: evt.ID.String(),
                    TargetEventID: resp.EventID.String(),
                    SourceRoomID:  evt.RoomID.String(),
                    TargetRoomID:  targetRoom.String(),
                    CreatedAt:     time.Now().UTC(),
                }
                err = storeMessageMapping(db, mapping)
                if err != nil {
                    log.Printf("Error storing message mapping: %v", err)
                }
            }
        }
    }
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
