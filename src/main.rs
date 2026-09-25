#[macro_use] extern crate rocket;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use futures_util::{SinkExt, StreamExt};
use rocket::serde::json::serde_json;
use rocket::State;
use serde::{Deserialize, Serialize};
use ws::{Channel, WebSocket};
use tokio::sync::{broadcast, RwLock, mpsc};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChatMessage {
    message_type: String,
    content: String,
    username: String,
    users: Vec<String>,
    to: Option<String>
}

struct Client {
    username: String,
    sender: mpsc::UnboundedSender<ChatMessage>,
}

struct Room {
    sender: broadcast::Sender<ChatMessage>,
    clients: HashMap<String, Client>
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
        self.rooms
            .entry(room_id.to_string())
            .or_insert_with(|| {
                let (sender, _) = broadcast::channel(100);
                Room {
                    sender,
                    clients: HashMap::new()
                }
            })
    }
}

#[get("/ws/<room_id>/<user_name>")]
fn websocket(ws: WebSocket, room_id: &str, user_name: &str, manager: &State<Arc<RwLock<RoomManager>>>) -> Channel<'static>{

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

            let (sender, users) = {
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

                (room.sender.clone(), users)

            };

            // Subscribe to room broadcast
            let mut receiver = sender.subscribe();

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
                                println!("[Room:{}] [{}] Received: {:?}", room_id, user_name, message);

                                if let Ok(text) = message.to_text() {
                                   #[derive(Debug, Deserialize)]
                                    struct IncomingMessage {
                                       message_type: String,
                                       content: String,
                                       to: Option<String>
                                   }
                                    match serde_json::from_str::<IncomingMessage>(text) {

                                       Ok(incoming) => {

                                           // =================================
                                           // Normal room message
                                           // =================================
                                           if incoming.message_type == "message" {
                                                   let chat_message = ChatMessage {
                                                   message_type: incoming.message_type,
                                                   content: incoming.content,
                                                   username: user_name.clone(),
                                                   users: Vec::new(),
                                                   to: None
                                               };

                                                let _ = sender.send(chat_message);
                                           }

                                           // =================================
                                           // Private message
                                           // =================================
                                            else if incoming.message_type == "private_message" {

                                               if let Some(target_username) = incoming.to {
                                                   let target_senders = {
                                                       let manager = manager.read().await;

                                                       manager.rooms.get(&room_id).map(|room| {
                                                           room.clients
                                                           .values()
                                                           .filter(|client| client.username == target_username)
                                                           .map(|client| client.sender.clone())
                                                           .collect::<Vec<_>>()
                                                       })
                                                       .unwrap_or_default()
                                                   };

                                                   let private_message = ChatMessage {
                                                       message_type: "private_message".to_string(),
                                                       username: user_name.clone(),
                                                       content: incoming.content,
                                                       users: Vec::new(),
                                                       to: Some(target_username.clone())
                                                   };

                                                   for target_sender in target_senders {
                                                       let _ = target_sender.send(private_message.clone());
                                                   }

                                               }
                                               else {
                                                   println!("[Room: {}] Unknown message type: {}", room_id, incoming.message_type);
                                               }
                                           }
                                       }
                                       Err(error) => {
                                           println!("[Room: {}] Invalid JSON: {}", room_id, error);
                                       }
                                    }
                                }
                            }
                            Some(Err(error)) => {
                                println!("[Room: {}] WebSocket error: {}", room_id, error);
                                break;
                            }
                            None => {
                                println!("[Room: {}] [{}] Client disconnected", user_name, room_id);
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