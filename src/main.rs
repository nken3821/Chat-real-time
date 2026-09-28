#[macro_use]
extern crate rocket;

use futures_util::{SinkExt, StreamExt};
use rocket::State;
use rocket::serde::json::serde_json;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, mpsc};
use uuid::Uuid;
use ws::{Channel, WebSocket};

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage {
    message_type: String,
    content: String,
    username: String,
    users: Vec<String>,
    to: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MessageType {
    Message,
    PrivateMessage,
}

#[derive(Debug, Deserialize)]
struct IncomingMessage {
    message_type: MessageType,
    content: String,
    to: Option<String>,
}

struct Client {
    username: String,
    sender: mpsc::UnboundedSender<ChatMessage>,
}

struct Room {
    sender: broadcast::Sender<ChatMessage>,
    clients: HashMap<String, Client>,
    history: VecDeque<ChatMessage>,
}

struct RoomManager {
    rooms: HashMap<String, Room>,
}

impl RoomManager {
    fn new() -> Self {
        Self {
            rooms: HashMap::new(),
        }
    }

    fn get_or_create_room(&mut self, room_id: &str) -> &mut Room {
        self.rooms.entry(room_id.to_string()).or_insert_with(|| {
            let (sender, _) = broadcast::channel(100);
            Room {
                sender,
                clients: HashMap::new(),
                history: VecDeque::new(),
            }
        })
    }
}

#[get("/ws/<room_id>/<user_name>")]
fn websocket(
    ws: WebSocket,
    room_id: &str,
    user_name: &str,
    manager: &State<Arc<RwLock<RoomManager>>>,
) -> Channel<'static> {
    let connection_id = Uuid::new_v4().to_string();

    let manager = manager.inner().clone();
    let room_id = room_id.to_string();
    let user_name = user_name.to_string();

    ws.channel(move |mut stream| {
        Box::pin(async move {

            // =========================================
            // Create private channel for this client
            // =========================================

            let (private_sender, mut private_receiver) = mpsc::unbounded_channel::<ChatMessage>();

            // =========================================
            // Join room
            // =========================================

            let (sender, users, history) = {
                let mut manager = manager.write().await;

                let room = manager.get_or_create_room(&room_id);
                room.clients.insert(
                    connection_id.clone(),
                    Client {
                        username: user_name.clone(),
                        sender: private_sender
                    });


                // online users
                let users = room.clients.values()
                    .map(|client| client.username.clone())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();

                let history = room.history.iter().cloned().collect::<Vec<_>>();

                (room.sender.clone(), users, history)
            };

            // Subscribe to room broadcast
            let mut receiver = sender.subscribe();

            for history_manager in history {
                match serde_json::to_string(&history_manager) {
                    Ok(json) => {
                        if let Err(error) = stream.send(json.into()).await {
                            println!("[Room: {}] History send error: {}", room_id, error);
                           break;
                        }
                    }
                    Err(error) => {
                        println!("[Room: {}] History serialization error: {}", room_id, error);
                    }
                }
            }

            // Notify room that user joined
            let _ = sender.send(ChatMessage {
                message_type: "join".to_string(),
                username: user_name.clone(),
                content: String::new(),
                users: users.clone(),
                to: None
            });

            // =========================================
            // Send current users only to this client
            // =========================================
            let users_message = ChatMessage {
                message_type: "users".to_string(),
                username: String::new(),
                content: String::new(),
                users,
                to: None
            };

            if let Ok(json) = serde_json::to_string(&users_message) {
                let _ = stream.send(json.into()).await;
            }

            // =========================================
            // Main loop
            // =========================================
            loop {
               tokio::select! {

                   // =================================
                   // 1. Client -> Server
                   // =================================
                    message = stream.next() => {
                        match message {

                            Some(Ok(message)) => {

                                println!(
                                    "[Room:{}] [{}] Received: {:?}",
                                    room_id,
                                    user_name,
                                    message
                                );

                                if let Ok(text) = message.to_text() {

                                    match serde_json::from_str::<IncomingMessage>(text) {

                                        Ok(incoming) => {

                                            // =================================
                                            // Validate content
                                            // =================================

                                            if incoming.content.trim().is_empty() {
                                                println!(
                                                    "[Room: {}] Empty message rejected",
                                                    room_id
                                                );

                                                continue;
                                            }

                                            // =================================
                                            // Handle message type
                                            // =================================

                                            match incoming.message_type {

                                                // =================================
                                                // Normal room message
                                                // =================================

                                                MessageType::Message => {

                                                    let chat_message = ChatMessage {
                                                        message_type: "message".to_string(),
                                                        content: incoming.content,
                                                        username: user_name.clone(),
                                                        users: Vec::new(),
                                                        to: None,
                                                    };

                                                    // Save to history
                                                    {
                                                        let mut manager =
                                                            manager.write().await;

                                                        if let Some(room) =
                                                            manager.rooms.get_mut(&room_id)
                                                        {
                                                            room.history.push_back(
                                                                chat_message.clone()
                                                            );

                                                            // Keep only last 50 messages
                                                            if room.history.len() > 50 {
                                                                room.history.pop_front();
                                                            }
                                                        }
                                                    }

                                                    // Broadcast to room
                                                    let _ = sender.send(chat_message);
                                                }

                                                // =================================
                                                // Private message
                                                // =================================

                                                MessageType::PrivateMessage => {

                                                    let target_username =
                                                        match incoming.to {

                                                            Some(username) => username,

                                                            None => {
                                                                println!(
                                                                    "[Room: {}] Private message missing target",
                                                                    room_id
                                                                );

                                                                continue;
                                                            }
                                                        };

                                                    // Find ALL connections of target user
                                                    let target_senders = {

                                                        let manager =
                                                            manager.read().await;

                                                        manager
                                                            .rooms
                                                            .get(&room_id)
                                                            .map(|room| {

                                                                room.clients
                                                                    .values()
                                                                    .filter(|client| {
                                                                        client.username
                                                                            == target_username
                                                                    })
                                                                    .map(|client| {
                                                                        client.sender.clone()
                                                                    })
                                                                    .collect::<Vec<_>>()

                                                            })
                                                            .unwrap_or_default()
                                                    };

                                                    // Create private message
                                                    let private_message = ChatMessage {
                                                        message_type:
                                                            "private_message".to_string(),

                                                        username:
                                                            user_name.clone(),

                                                        content:
                                                            incoming.content,

                                                        users:
                                                            Vec::new(),

                                                        to:
                                                            Some(
                                                                target_username.clone()
                                                            ),
                                                    };

                                                    // Send to all target connections
                                                    for target_sender in target_senders {

                                                        let _ =
                                                            target_sender.send(
                                                                private_message.clone()
                                                            );
                                                    }
                                                }
                                            }
                                        }

                                        Err(error) => {

                                            println!(
                                                "[Room: {}] Invalid JSON: {}",
                                                room_id,
                                                error
                                            );
                                        }
                                    }
                                }
                            }

                            Some(Err(error)) => {

                                println!(
                                    "[Room: {}] WebSocket error: {}",
                                    room_id,
                                    error
                                );

                                break;
                            }

                            None => {

                                println!(
                                    "[Room: {}] [{}] Client disconnected",
                                    room_id,
                                    user_name
                                );

                                break;
                            }
                        }
                    }

                    // =================================
                    // 2. Room broadcast -> Client
                    // =================================
                    message = receiver.recv() => {
                        match message {
                            Ok(chat_message) => {
                               match serde_json::to_string(&chat_message) {
                                   Ok(json) => {
                                       if let Err(error) = stream.send(json.into()).await {
                                           println!("[Room: {}] Send error: {}", room_id, error);
                                           break;
                                       }
                                   }
                                    Err(error) => {
                                       println!("[Room: {}] Serialization error: {}", room_id, error);
                                   }
                               }

                            }
                            Err(error) => {
                                println!("[Room: {}] Broadcast error: {}", room_id, error);
                                break;
                            }
                        }
                    }

                    // =================================
                    // 3. Private message -> Client
                    // =================================
                    Some(private_message) = private_receiver.recv() => {
                        match serde_json::to_string(&private_message) {
                            Ok(json) => {
                                if let Err(error) = stream.send(json.into()).await {
                                    println!("[Room: {}] Private send error: {}", room_id, error);
                                    break;
                                }
                            }
                            Err(error) => {
                                println!("[Room: {}] Private serialization error: {}", room_id, error);
                            }
                        }
                    }
               }
            }

            // =========================================
            // Remove client from room
            // =========================================
            let users = {
                let mut manager = manager.write().await;

                if let Some(room) = manager.rooms.get_mut(&room_id) {
                    room.clients.remove(&connection_id);
                    room.clients
                        .values()
                        .map(|client| client.username.clone())
                        .collect::<HashSet<_>>()
                        .into_iter()
                        .collect::<Vec<_>>()
                }
                else {
                    Vec::new()
                }
            };

            // =========================================
            // Notify room about LEAVE
            // =========================================
            let _ = sender.send(ChatMessage {
                message_type: "leave".to_string(),
                username: user_name.clone(),
                content: String::new(),
                users,
                to: None
            });
            Ok(())
        })
    })
}

#[launch]
fn rocket() -> _ {
    let manager = Arc::new(RwLock::new(RoomManager::new()));

    rocket::build()
        .manage(manager)
        .mount("/", routes![websocket])
}
