package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "fmt"
    "log"
    "time"
    _ "github.com/mattn/go-sqlite3"
    "maunium.net/go/mautrix"
    "maunium.net/go/mautrix/event"
    "maunium.net/go/mautrix/id"
)





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

                                // Check if this is a threaded message
                                if content.RelatesTo.Type == "m.thread" {
                                        log.Printf("Detected thread message")

                                        // Step 1: Get the thread start event ID
                                        threadStartEventID := string(content.RelatesTo.EventID)
                                        if threadStartEventID == "" {
                                                log.Printf("Error: Thread start EventID is empty")
                                                return
                                        }

                                        // Step 2: Get the in-reply-to event ID
                                        inReplyToEventID := ""
                                        if content.RelatesTo.InReplyTo != nil {
                                                inReplyToEventID = string(content.RelatesTo.InReplyTo.EventID)
                                        }
                                        if inReplyToEventID == "" {
                                                log.Printf("Error: In-Reply-To EventID is empty")
                                                return
                                        }

                                        // Step 3: Map the thread start event ID
                                        mappedThreadStartEvent, err := getMessageMapping(db, id.EventID(threadStartEventID))
                                        if err != nil {
                                                log.Printf("Error fetching mapping for thread start event %s: %v", threadStartEventID, err)
                                                return
                                        }
                                        if mappedThreadStartEvent.TargetEventID == "" {
                                                log.Printf("No mapping found for thread start event %s in destination room", threadStartEventID)
                                                return
                                        }

                                        // Step 4: Map the in-reply-to event ID
                                        mappedInReplyToEvent, err := getMessageMapping(db, id.EventID(inReplyToEventID))
                                        if err != nil {
                                                log.Printf("Error fetching mapping for in-reply-to event %s: %v", inReplyToEventID, err)
                                                return
                                        }
                                        if mappedInReplyToEvent.TargetEventID == "" {
                                                log.Printf("No mapping found for in-reply-to event %s in destination room", inReplyToEventID)
                                                return
                                        }

                                        // Step 5: Update the event IDs in the content
                                        content.RelatesTo.EventID = id.EventID(mappedThreadStartEvent.TargetEventID)
                                        if content.RelatesTo.InReplyTo != nil {
                                                content.RelatesTo.InReplyTo.EventID = id.EventID(mappedInReplyToEvent.TargetEventID)
                                        }
                                        log.Printf("Updated thread start EventID to %s and in-reply-to EventID to %s",
                                                mappedThreadStartEvent.TargetEventID,
                                                mappedInReplyToEvent.TargetEventID,
                                        )

                                } else {
                                        // Handle regular replies
                                        log.Printf("Detected reply message")

                                        // Step 1: Get the related event ID
                                        relatedEventID := ""
                                        if content.RelatesTo.InReplyTo != nil {
                                                relatedEventID = string(content.RelatesTo.InReplyTo.EventID)
                                        } else {
                                                relatedEventID = string(content.RelatesTo.EventID)
                                        }

                                        if relatedEventID == "" {
                                                log.Printf("Error: Related EventID is empty or not found")
                                                return
                                        }

                                        // Step 2: Map the related event ID
                                        bridgedEvent, err := getMessageMapping(db, id.EventID(relatedEventID))
                                        if err != nil {
                                                log.Printf("Error fetching mapping for related event %s: %v", relatedEventID, err)
                                                return
                                        }
                                        if bridgedEvent.TargetEventID == "" {
                                                log.Printf("No mapping found for related event %s in destination room.", relatedEventID)
                                                return
                                        }

                                        // Step 3: Update the related event ID to the mapped event ID
                                        if content.RelatesTo.InReplyTo != nil {
                                                content.RelatesTo.InReplyTo.EventID = id.EventID(bridgedEvent.TargetEventID)
                                        } else {
                                                content.RelatesTo.EventID = id.EventID(bridgedEvent.TargetEventID)
                                        }
                                        log.Printf("Updated related event ID to %s", bridgedEvent.TargetEventID)
                                }
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





// Reactions
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


// Redactions
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


//Membership events like joins, leaves, nick or pfp change
func handleMemberEvent(ctx context.Context, evt *event.Event, client *mautrix.Client,
    roomLinks map[id.RoomID]id.RoomID, roomClients map[id.RoomID]*mautrix.Client, processedEventCache *ProcessedEventsCache) {

    // Check for duplicate events
    if processedEventCache.Contains(string(evt.ID)) {
        log.Printf("[%s] Skipping already processed event: %s", client.UserID, evt.ID)
        return
    }
    processedEventCache.Add(string(evt.ID))

    // Ignore events from before the application started
    if !isEventAfterStartTime(evt) {
        return
    }

    // Check if the event is linked to a target room
    targetRoom, ok := roomLinks[evt.RoomID]
    if !ok {
        log.Printf("No target room linked for event in room %s", evt.RoomID)
        return
    }

    targetClient := roomClients[targetRoom]
    if targetClient == nil {
        log.Printf("No target client found for target room %s", targetRoom)
        return
    }

    // Parse the event content and previous content safely
    var content, prevContent *event.MemberEventContent
    if len(evt.Content.VeryRaw) > 0 {
        content = evt.Content.AsMember()
    }

    if evt.Unsigned.PrevContent != nil && len(evt.Unsigned.PrevContent.VeryRaw) > 0 {
        // Manually unmarshal prev_content
        prevContent = &event.MemberEventContent{}
        json.Unmarshal(evt.Unsigned.PrevContent.VeryRaw, prevContent)
    }

    if content == nil {
        log.Printf("[%s] Invalid or nil content for event: %s", client.UserID, evt.ID)
        return
    }

    // Determine the type of membership event
    var message string
    if prevContent == nil {
        // True join event (prev_content is missing)
        if content.Membership == event.MembershipJoin {
            message = fmt.Sprintf("🚪🏃‍➡️ %v Joined the room", evt.Sender)
        }
    } else {
        // Handle updates (prev_content exists)
        if content.Membership == event.MembershipJoin {
            avatarChanged := content.AvatarURL != prevContent.AvatarURL
            displayNameChanged := content.Displayname != prevContent.Displayname

            if avatarChanged && displayNameChanged {
                message = fmt.Sprintf("😼 %v Updated display name and profile picture", evt.Sender)
            } else if displayNameChanged {
                message = fmt.Sprintf("😼 %v Updated display name", evt.Sender)
            } else if avatarChanged {
                message = fmt.Sprintf("😼 %v Updated profile picture", evt.Sender)
            }

            // If no relevant changes, skip the event
            if message == "" {
                return
            }
        } else if content.Membership == event.MembershipLeave {
            // Handle leave events
            message = fmt.Sprintf("🚪🏃 %v Left the room", evt.Sender)
        } else {
            // Unknown membership event
            return
        }
    }

    // Fetch the sender's profile (avatar URL and display name)
    displayName, avatarURL, err := fetchSenderProfile(ctx, client, evt.Sender)
    if err != nil {
        log.Printf("[%s] Error fetching sender profile for %s: %v", client.UserID, evt.Sender, err)
        displayName = "Default-Bridge"
        avatarURL = "mxc://default/avatar"
    }

    // Construct the message content
    contentMap := map[string]interface{}{
        "msgtype": "m.notice",
        "body":    message,
        "com.beeper.per_message_profile": map[string]interface{}{
            "avatar_url":  avatarURL,
            "displayname": displayName,
            "id":          evt.Sender.String(),
        },
    }

    // Send the message to the target room
    _, sendErr := targetClient.SendMessageEvent(ctx, targetRoom, event.EventMessage, contentMap)
    if sendErr != nil {
        log.Printf("[%s] Error sending member event message to target room: %v", client.UserID, sendErr)
    }
}






// Update your main function's event handler registrations to:
func registerEventHandlers(syncer *mautrix.DefaultSyncer, client *mautrix.Client,
    roomLinks map[id.RoomID]id.RoomID, roomClients map[id.RoomID]*mautrix.Client, db *sql.DB, roomChans map[id.RoomID]chan RoomEvent) {

    // Register event handlers for each client
    syncer.OnEventType(event.EventMessage, func(ctx context.Context, evt *event.Event) {
        if ch, ok := roomChans[evt.RoomID]; ok {
            ch <- RoomEvent{Ctx: ctx, Event: evt, Type: event.EventMessage}
        }
    })

    syncer.OnEventType(event.EventReaction, func(ctx context.Context, evt *event.Event) {
        if ch, ok := roomChans[evt.RoomID]; ok {
            ch <- RoomEvent{Ctx: ctx, Event: evt, Type: event.EventReaction}
        }
    })

    syncer.OnEventType(event.EventRedaction, func(ctx context.Context, evt *event.Event) {
        if ch, ok := roomChans[evt.RoomID]; ok {
            ch <- RoomEvent{Ctx: ctx, Event: evt, Type: event.EventRedaction}
        }
    })

    log.Printf("Registering membership event handler for event type: %s", event.StateMember)
    syncer.OnEventType(event.StateMember, func(ctx context.Context, evt *event.Event) {
        if ch, ok := roomChans[evt.RoomID]; ok {
            ch <- RoomEvent{Ctx: ctx, Event: evt, Type: event.StateMember}
        }
    })
}
