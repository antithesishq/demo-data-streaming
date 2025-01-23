use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use clap::{App, Arg};
use log::{info, error, debug};

use rdkafka::config::ClientConfig;
use rdkafka::message::{OwnedHeaders, Header, Headers};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::get_rdkafka_version;
use env_logger;

use reqwest;

use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;

use std::net::SocketAddr;
use rdkafka::producer::BaseProducer;

use axum::{
    body::Body,
    extract::{State, Json, Extension, Path},
    middleware::{self, Next},
    response::{Response, Json as ResponseJson},
    routing::post,
    Router,
};

use serde::{Serialize, Deserialize};
use serde_json::Value;
use serde_json::json;

use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Debug)]
struct Event {
    transaction: String,
    time: Duration
}

// Define shared state
#[derive(Clone)]
struct AppState {
    producers: Arc<Mutex<HashMap<String, FutureProducer>>>,
    producers_histories: Arc<Mutex<HashMap<String, Vec<Event>>>>,
    brokers: Vec<&'static str>,
    // faker_endpoints: Vec<&'static str>
}

#[derive(Clone, Debug, EnumIter)]
enum KafkaProducers {
    ExactlyOnce,
    AtLeastOnce,
    AtLeastOnceBatch
}

impl KafkaProducers {
    fn create_config(&self, brokers: &Vec<&str>) -> FutureProducer {
        match self {
            KafkaProducers::ExactlyOnce => {
                ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("debug", "all")
                    .set("retries", "5") // For exactly-once, higher retries
                    .set("acks", "all")  // Ensure all replicas acknowledge
                    .create::<FutureProducer>()
                    .expect("atleast once producer creation error")
            },
            KafkaProducers::AtLeastOnce => {
                ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("debug", "all")
                    .create()
                    .expect("exactly once producer creation error")
            },
            KafkaProducers::AtLeastOnceBatch => {
                ClientConfig::new()
                    .set("bootstrap.servers", brokers.join(","))
                    .set("message.timeout.ms", "5000")
                    .set("debug", "all")
                    .set("batch.size", "16384") // 16 KB batch size
                    .set("linger.ms", "5") // Wait for 5ms before sending a batch, useful with for loops
                    .set("buffer.memory", "33554432") // 32 MB buffer size
                    .set("compression.type", "gzip") // Use gzip compression
                    .create()
                    .expect("atleast once batch producer creation error")
            }
        }
    }

    fn get_route(&self) -> &'static str {
        match self {
            KafkaProducers::ExactlyOnce => "/exactly_once",
            KafkaProducers::AtLeastOnce => "/atleast_once",
            KafkaProducers::AtLeastOnceBatch => "/atleast_once_batch"
        }
    }
}

#[derive(Deserialize, Serialize)]
#[derive(Debug)]
struct BankAccount {
    aba: String,
    iban: String,
    swift11: String,
    bank_country: String
}

// #[derive(Debug)]
// enum BankAccountResponse {
//     Single(BankAccount),
//     Multiple(Vec<BankAccount>),
// }

#[derive(Clone, Debug, EnumIter)]
enum Faker {
    Batch,
    Single,
    BatchSequential
    // Faker API
}

impl Faker {
    fn get_faker_url(&self) -> &'static str {
        match self {
            Faker::Batch => "http://data_generator:5000/batch",
            Faker::Single => "http://data_generator:5000/single",
            Faker::BatchSequential => "http://data_generator:5000/batch_sequential",
        }
    }

    fn create_full_faker_url(&self, url_end: Option<&str>) -> String {
        match url_end {
            Some(dynamic_data) => format!("{}/{}", self.get_faker_url(), dynamic_data),
            None => self.get_faker_url().to_string(),
        }
    }

    async fn make_request(&self, url_end: Option<&str>) -> reqwest::Response{
        let client = reqwest::Client::new();
        let url = self.create_full_faker_url(url_end);
        println!("{:?}", url);
        client
            .get(url)
            .send()
            .await
            .expect("Failed to send request")
    }
}


fn create_nested_router(state: Arc<AppState>) -> Router {
    let mut r = Router::new();
    for kafka_mode in KafkaProducers::iter() {
        r = r.nest(
            kafka_mode.get_route(),
            build_faker_to_kafka_router(state.clone(), kafka_mode)
        )
    }
    r
}


fn build_faker_to_kafka_router(state: Arc<AppState>, mode: KafkaProducers) -> Router{
     Router::new()
        .route("/batch/:topic/:num_records", post(handle_batch))
        .route("/single/:topic", post(handle_single))
        // .route("/batch_sequential/:topic/:", post(handle_sequential_batch))
        .with_state(state) // Attach state to the router
        .layer(middleware::from_fn(move |req, next| {
            add_context_middleware(req, next, mode.clone())
        }))
       
}

async fn add_context_middleware(
    mut req: axum::http::Request<Body>,
    next: Next,
    context: KafkaProducers, // Pass context explicitly
) -> Response {
    println!("middleware: adding context to request");
    req.extensions_mut().insert(context);
    next.run(req).await
}

async fn handle_batch(
    Path((topic, num_records)): Path<(String, u32)>,
    State(state): State<Arc<AppState>>,
    Extension(mode): Extension<KafkaProducers>
) {
    let mut producers = state.producers.lock().await;
    let mut producers_histories = state.producers_histories.lock().await;
    let brokers = &state.brokers;
    let bank_accounts = Faker::Batch.make_request(Some(&num_records.to_string())).await
        .json::<Vec<BankAccount>>()
        .await
        .expect("Failed to deserialize multiple BankAccounts");

    for bank_account in bank_accounts.into_iter() {
        let bank_account_str = serde_json::to_string(&bank_account)
            .expect("Failed to serialize BankAccount to JSON");
        let msg = format!("Message {}", &bank_account_str);
        let key = format!("Key {}", &bank_account.iban);
        producers
            .entry(mode.get_route().to_string())
            .or_insert(mode.create_config(brokers))
            .send(
                FutureRecord::to(&topic)
                    .payload(&msg)
                    .key(&key),
                Duration::from_secs(0),
            )
            .await
            .and_then(|delivery| {
                println!("middleware: adding context to request {:?}", delivery);
                producers_histories
                .entry(mode.get_route().to_string())
                .or_insert_with(Vec::new)
                .push(
                    Event {
                        transaction: bank_account_str.clone(),
                        time: SystemTime::now().duration_since(UNIX_EPOCH).expect("system time error")
                    }
                );
                Ok(())
            })
            .unwrap_or_else(|e| {
                error!("Failed to deliver message {:?}", e);
            });
    }

    // The producer will automatically batch before sending messages
    // Kafka itself handles batching efficiently at the partition level. 
    // By increasing the producer's throughput configurations, you can leverage Kafka's internal batching mechanisms
}

async fn handle_single(
    Path(topic): Path<String>,
    State(state): State<Arc<AppState>>,
    Extension(mode): Extension<KafkaProducers>
) {
    println!("Here");
    let mut producers = state.producers.lock().await;
    let mut producers_histories = state.producers_histories.lock().await;
    info!("producer_histories: {:?}", producers_histories);
    info!("producers: {:?}", producers.len());
    let brokers = &state.brokers;
    let bank_account = Faker::Single.make_request(None).await
        .json::<BankAccount>()
        .await
        .expect("Failed to deserialize BankAccount");
    info!("Data from Faker {:?}", bank_account);
    let bank_account_str = serde_json::to_string(&bank_account)
        .expect("Failed to serialize BankAccount to JSON");
    let msg = format!("Message {}", &bank_account_str);
    let key = format!("Key {}", &bank_account.iban);
    info!("Data being sent to producer: topic: {}, key: {}, message: {}", topic, key, msg);
    producers
        .entry(mode.get_route().to_string())
        .or_insert(mode.create_config(brokers))
        .send(
            FutureRecord::to(&topic)
                .payload(&msg)
                .key(&key),
            Duration::from_secs(0),
        )
        .await
        .and_then(|delivery| {
            info!("Message delivered to {:?}",  delivery);
            producers_histories
                .entry(mode.get_route().to_string())
                .or_insert_with(Vec::new)
                .push(
                    Event {
                        transaction: bank_account_str.clone(),
                        time: SystemTime::now().duration_since(UNIX_EPOCH).expect("system time error")
                    }
                );
            Ok(())
            // Save here with timestamp
        })
        .unwrap_or_else(|e| {
            error!("Failed to deliver message {:?}", e);
        });
    // todo: await producer response,
    // if good add to producer history yipee!
}

// API endpoint to get selected mode's history etc. this endpoint will be called by the consumer to make sure everything has been consumed

#[tokio::main]
async fn main() {
    println!("{:?}", SystemTime::now().duration_since(UNIX_EPOCH));
    env_logger::init();

    let producers: Arc<Mutex<HashMap<String, FutureProducer>>> = Arc::new(Mutex::new(HashMap::new()));
    let producers_histories: Arc<Mutex<HashMap<String, Vec<Event>>>> = Arc::new(Mutex::new(HashMap::new()));
    let brokers = vec!["kafka-3:9092", "kafka-2:9092", "kafka-1:9092"];

    let state = AppState { producers, producers_histories, brokers };
    let app = create_nested_router(state.into());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running at http:{:?}", listener);
    
    axum::serve(listener, app).await.unwrap();    
}