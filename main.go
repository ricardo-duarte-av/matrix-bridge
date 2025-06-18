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

// Global processed events cache
var processedEventCache *ProcessedEventsCache


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


func getClientForUser(user string, clients map[string]*mautrix.Client) *mautrix.Client {
    for _, client := range clients {
        if client.UserID.String() == user {
            return client
        }
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

    // Initialize the processed events cache
    processedEventCache = NewProcessedEventsCache(1000)


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

    // Per-room event channels
    type RoomEvent struct {
        Ctx   context.Context
        Event *event.Event
        Type  event.Type
    }
    roomChans := make(map[id.RoomID]chan RoomEvent)
    var wg sync.WaitGroup

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

        // Per-room event channel and goroutine
        if _, exists := roomChans[fromRoom]; !exists {
            ch := make(chan RoomEvent, 100)
            roomChans[fromRoom] = ch
            wg.Add(1)
            go func(roomID id.RoomID, ch chan RoomEvent) {
                defer wg.Done()
                for re := range ch {
                    switch re.Type {
                    case event.EventMessage:
                        handleMessageEvent(re.Ctx, re.Event, roomClients[roomID], roomLinks, roomClients, db)
                    case event.EventReaction:
                        handleReactionEvent(re.Ctx, re.Event, roomClients[roomID], roomLinks, roomClients, db)
                    case event.EventRedaction:
                        handleRedactionEvent(re.Ctx, re.Event, roomClients[roomID], roomLinks, roomClients, db)
                    case event.StateMember:
                        handleMemberEvent(re.Ctx, re.Event, roomClients[roomID], roomLinks, roomClients, processedEventCache)
                    }
                }
            }(fromRoom, ch)
        }

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

        // Register event handlers with per-room channel dispatch
        registerEventHandlers(syncer, c, roomLinks, roomClients, db, roomChans)

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
                                    event.StateMember, // Include membership events
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

    // Close all room channels and wait for goroutines
    for _, ch := range roomChans {
        close(ch)
    }
    wg.Wait()

    cancel()
}
