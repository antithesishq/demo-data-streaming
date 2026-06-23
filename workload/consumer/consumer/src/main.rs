use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use clap::{App, Arg};
use log::{info, error, debug, warn};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::message::{OwnedHeaders, Header, Headers};
use rdkafka::{ClientContext};
use rdkafka::consumer::{Consumer, StreamConsumer, Rebalance, BaseConsumer, ConsumerContext, CommitMode};
use rdkafka::util::get_rdkafka_version;
use rdkafka::TopicPartitionList;
use rdkafka::error::KafkaResult;
use rdkafka::Message;

use env_logger;

use reqwest;

use tokio::sync::Mutex;
use tracing_subscriber::fmt::format;
use std::sync::Arc;
use std::collections::HashMap;
// use std::error::Error;

use std::net::SocketAddr;

use axum::{
    body::Body,
    extract::{State, Json, Extension, Path},
    middleware::{self, Next},
    response::{Response, Json as ResponseJson, IntoResponse},
    http::StatusCode,
    routing::post,
    Router,
};

use antithesis_sdk::prelude::*;

use serde::{Serialize, Deserialize};
use serde_json::Value;
use serde_json::json;
use serde_postgres::de::from_row;

use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio_postgres::{NoTls, Row};
use tokio_postgres::types::ToSql;

use std::thread::sleep;
use chrono::NaiveDateTime;

use postgres_types::{Type, IsNull};
use anyhow::{Result, Context};


use chrono;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::{
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, Update, TransactWriteItem
    }, 
};
use aws_sdk_dynamodb::config::Credentials;
use aws_sdk_dynamodb::config::SharedCredentialsProvider;
//use aws_types::SharedCredentialsProvider;
//use aws_types::Credentials;


fn is_retryable_error(error_msg: &str) -> bool {
    // Common retryable DynamoDB errors
    error_msg.contains("ProvisionedThroughputExceededException") ||
    error_msg.contains("ThrottlingException") ||
    error_msg.contains("InternalServerError") ||
    error_msg.contains("ServiceUnavailable") ||
    error_msg.contains("RequestLimitExceeded") ||
    error_msg.contains("TransactionConflictException")
}

#[derive(Debug)]
struct DynamoDb {
    client: aws_sdk_dynamodb::Client
}

impl DynamoDb {
    async fn new() -> Result<Self> {
        //let credentials = Credentials::from_keys("dummy", "dummy", None);
        let credentials = Credentials::new(
            "dummy",
            "dummy",
            None,
            None,
            "dummy",
        );
        let shared = SharedCredentialsProvider::new(credentials);
        let config = aws_config::from_env()
            .region(RegionProviderChain::default_provider().or_else("us-east-1"))
            .credentials_provider(shared)
            .endpoint_url("http://ddb:8000") 
            .load()
            .await;
        let client = aws_sdk_dynamodb::Client::new(&config);
        Ok(DynamoDb { client })
    }

    async fn safe_create_table(&self){
        let table_name = "accounts";
        
        let ad = AttributeDefinition::builder()
            .attribute_name("iban")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .unwrap();

        let ks = KeySchemaElement::builder()
            .attribute_name("iban")  
            .key_type(KeyType::Hash) 
            .build()
            .unwrap(); 

        loop {
            match self.client
            .create_table()
            .table_name(table_name)
            .key_schema(ks.clone())
            .attribute_definitions(ad.clone())
            .billing_mode(BillingMode::PayPerRequest)
            .send()
            .await
            {
                Ok(_) => {
                    println!("dynamodb: table '{}' created successfully", table_name);
                    break;
                },
                Err(e) => {
                    if e.to_string().contains("ResourceInUseException") {
                        println!("dynamodb: error: table '{}' already exists", table_name);
                        break;
                    } else {
                        println!("dynamodb: error: failed to create table {:?}, retrying ...", e);
                        sleep(Duration::from_secs(1)); // Avoid tight retry loop 
                    }
                }
            }
        }    
    }

    async fn safe_transaction(&self, b_d: BankData) {
        let b_t = BankTransaction::from_bank_data(b_d);
        match b_t {
            Ok(BankTransaction::BankTransfer { from, to, amount, .. }) => {
                self.safe_transfer(from, to, amount).await;

            },
            Ok(BankTransaction::BankFund{ iban, amount, .. }) => {
                self.safe_fund(iban, amount).await;
            },
            Err(e) => eprintln!("dynamodb: failed to deserialize bank_transaction data {:?}", e)
            
        }
    }

    async fn safe_fund(&self, iban: String, amount: f64) {
        // Idempotent upsert fund.
        //
        // We must not assume the account row was created by this fund: a transfer
        // (`ADD balance`) can be consumed before the matching fund and auto-create
        // the row. The old conditional `Put` with `attribute_not_exists(iban)` would
        // then fail against that pre-existing row and the initial funding would be
        // lost forever -> `sum(balances) < num_accounts * 1000` (money conservation
        // violation).
        //
        // Instead, `ADD balance :amount` credits the initial funding whether or not
        // the row already exists (creating it if it doesn't), so a late fund still
        // lands on top of whatever balance a prior transfer left. A `funded` marker
        // guarded by `attribute_not_exists(funded)` keeps the operation idempotent:
        // a redelivered fund message (at-least-once) is a no-op rather than a
        // double-credit.
        let fund = match Update::builder()
            .table_name("accounts")
            .key("iban", AttributeValue::S(iban.clone()))
            .update_expression("SET funded = :funded ADD balance :amount")
            .condition_expression("attribute_not_exists(funded)")
            .expression_attribute_values(":amount", AttributeValue::N(amount.to_string()))
            .expression_attribute_values(":funded", AttributeValue::Bool(true))
            .build() {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("dynamodb: failed to build fund transaction, error: {:?}", e);
                    return;
                }
            };
        let transact_items = vec![
            TransactWriteItem::builder().update(fund).build(),
        ];

        loop {
            match self.client
                .transact_write_items()
                .set_transact_items(Some(transact_items.clone()))
                .send()
                .await
            {
                Ok(_) => {
                    println!("dynamodb: account funding successful: {} funded with {}", iban, amount);
                    break;
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("ConditionalCheckFailed") {
                        // Account already funded (duplicate/redelivered message) -> idempotent no-op.
                        println!("dynamodb: account {} already funded, skipping (idempotent)", iban);
                        break;
                    } else if is_retryable_error(&error_msg) {
                        eprintln!("dynamodb: fund transaction failed with error: {}, retrying ...", error_msg);
                        sleep(Duration::from_secs(1)); // Avoid tight retry loop
                    } else {
                        eprintln!("dynamodb: fatal: non-retryable error encountered: {}", error_msg);
                        return
                    }
                }
            }
        }

    }

    async fn safe_transfer(&self, from: String, to: String, amount: f64) {
        let from_transfer = match Update::builder()
                .table_name("accounts")
                .key("iban", AttributeValue::S(from.clone()))
                .update_expression("ADD balance :amount")
                .expression_attribute_values(":amount", AttributeValue::N((-amount).to_string()))
                .build() {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("dynamodb: failed to build 'from' account: {} update, error: {:?}", from, e);
                        return;
                    }
                };
                
        let to_transfer = match Update::builder()
                .table_name("accounts")
                .key("iban", AttributeValue::S(to.clone()))
                .update_expression("ADD balance :amount")
                .expression_attribute_values(":amount", AttributeValue::N(amount.to_string()))
                .build() {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("dynamodb: failed to build 'to' account: {} update, error: {}", to, e);
                        return;
                    }
                }; 

        let transact_items = vec![
            TransactWriteItem::builder().update(from_transfer).build(), 
            TransactWriteItem::builder().update(to_transfer).build(),
        ]; 
        
        loop { 
            match self.client
                .transact_write_items()
                .set_transact_items(Some(transact_items.clone()))
                .send()
                .await
            {
                Ok(_) => {
                    println!("dynamodb: transfer successful : {} from {} to {}", amount, &from, &to);
                    break
                }
                Err(e) => {
                    let error_msg = e.to_string(); 
                    if is_retryable_error(&error_msg) {
                        println!("dynamodb: transaction failed with error: {}, retrying ...", error_msg);
                        sleep(Duration::from_secs(1)); // Avoid tight retry loop
                    } else {
                        eprintln!("dynamodb: fatal error encountered: {}", error_msg);
                        return
                    }
                }
            }
        }
    } 
}

#[derive(Debug)]
struct Postgres {
    client: tokio_postgres::Client
}

impl Postgres {
    pub async fn new(connection_string: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(connection); // Spawn the connection as a background task
        Ok(Postgres { client })
    }

    async fn safe_write_consumed(&self, b_d_data: &(BankData, NaiveDateTime), kafka_mode: &KafkaConsumers) {
        let b_d = &b_d_data.0;
        let current_timestamp = &b_d_data.1;
        let update = "
            UPDATE producer
            SET 
                consumed_timestamp = $1,
                consumer_type = $2,
                consumed_count = consumed_count + 1
            WHERE id = $3;
        ";
        loop {
            // this performs update and returns number of rows updated. Not super useful for us, so we don't capture this number
            let result = self.client
                .execute(update, &[&current_timestamp, &kafka_mode.get_route(), &(b_d.id)]) // Assuming id is SERIAL (i32 in DB)
                .await;
            match result {
                Ok(d) => {
                    if d > 0 {
                        println!("postgres: Bank Account ID: {:?}, Updated timestamp: {}, Updated route: {}, result: d: {}", &b_d.id, &current_timestamp, &kafka_mode.get_route(), &d);
                        assert_sometimes!(true, "Recorded data consumed from Kafka to state-tracker", &json!({"result": b_d}));
                        break;
                    } else {
                        eprintln!("postgres: I guess we updated nothing...: {}", &d);
                        assert_unreachable!("Recorded 0 data consumed from Kafka to state-tracker with no errors", &json!({"result": "none"}));
                        sleep(Duration::from_secs(1)); // Avoid tight retry loop
                    }            
                },
                Err(e) => {
                    eprintln!("postgres: Failed to update timestamp: {}", e);
                    assert_sometimes!(false, "Recorded data consumed from Kafka to state-tracker", &json!({"error": format!("Failed to update timestamp {}", e)}));
                    sleep(Duration::from_secs(1)); // Avoid tight retry loop
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum BankTransaction {
    BankTransfer { to: String, from: String, amount: f64 },
    BankFund { iban: String, aba: String, swift11: String, bank_country: String, amount: f64 }
}

impl BankTransaction {
    fn from_bank_data(b_d: BankData) -> Result<Self, serde_json::Error> {
        // let b_d_topic: &str = &b_d.topic;
        let b_transaction: BankTransaction = serde_json::from_str(&b_d.data)
            .inspect_err(|e| {
                println!("kafka: transaction data failed to deserialize: {:?}", e);
                assert_sometimes!(false, "Consumer's consumed sub-message failed to deserialize to BankTransaction", &json!({ "error": format!("{:?}", e) }));
            })?;
        println!("kafka: bank transaction: {:?}", b_transaction);
        Ok(b_transaction)
    }
}



#[derive(Debug, Serialize, Deserialize, Clone)]
struct BankData {
    id: i32,
    data: String, 
    produced_timestamp: Option<SystemTime>,
    producer_type: Option<String>,
    consumed_timestamp: Option<SystemTime>,
    consumer_type: Option<String>,
    consumed_count: i32, 
    topic: String
    // consumed: bool
}

impl BankData {
    fn from_row(row: &Row) -> Self {
        BankData {
            id: row.get("id"),
            data: row.get("data"), 
            produced_timestamp: row.try_get("produced_timestamp").ok(),
            producer_type: row.try_get("producer_type").ok(),
            consumed_timestamp: row.try_get("consumed_timestamp").ok(),
            consumer_type: row.try_get("consumer_type").ok(),
            consumed_count: row.get("consumed_count"),
            topic: row.get("topic"),
        }
    } 
}



// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance) {
        println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }
}

// A type alias of custom consumer (for convenience).
type LoggingConsumer = StreamConsumer<CustomContext>;

#[derive(Clone, Debug, EnumIter)]
enum KafkaConsumers {
    ExactlyOncePassThroughConsumer,
    AtleastOncePassThroughConsumer
}

impl KafkaConsumers {
    fn get_route(&self) -> &'static str {
        match self {
            KafkaConsumers::ExactlyOncePassThroughConsumer => "/exactly_once_pass_through_consumer",
            KafkaConsumers::AtleastOncePassThroughConsumer => "/atleast_once_pass_through_consumer"
        }
    }
}

struct KafkaConsumer {
    kind: KafkaConsumers,
    consumer: Option<LoggingConsumer>
}


impl KafkaConsumer {
    fn create_config(&mut self, brokers: &Vec<&str>) {
        let config = match self.kind {
            KafkaConsumers::AtleastOncePassThroughConsumer => {
                let c = ClientConfig::new()
                    .set("group.id", "1")
                    .set("bootstrap.servers", brokers.join(","))
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "30000")
                    .set("enable.auto.commit", "true")
                    //.set("statistics.interval.ms", "30000")
                    .set("auto.offset.reset", "earliest")
                    .set_log_level(RDKafkaLogLevel::Debug)
                    .create_with_context(CustomContext)
                    .expect("Consumer creation failed");
                println!("kafka: creating atleast once kafka consumer");
                assert_reachable!("Created \'Atleast Once\' passthrough Kafka consumer", &json!({}));
                c
            },
            KafkaConsumers::ExactlyOncePassThroughConsumer => {
                let c = ClientConfig::new()
                    .set("group.id", "2")
                    .set("bootstrap.servers", brokers.join(","))
                    .set("enable.partition.eof", "false")
                    .set("session.timeout.ms", "30000")
                    .set("isolation.level", "read_committed")
                    .set("enable.auto.commit", "false")        // we have to commit offset ourself
                    .set("auto.offset.reset", "earliest")
                    .set_log_level(RDKafkaLogLevel::Debug)
                    .create_with_context(CustomContext)
                    .expect("Consumer creation failed");
                println!("kafka: creating exactly once kafka consumer");
                assert_reachable!("Created \'Exactly Once\' passthrough Kafka consumer", &json!({}));
                c
            }
        };
        self.consumer = Some(config);
    }

    fn safe_subscribe(&mut self, topic: &str) {
        loop {
            let res = self.consumer.as_ref().unwrap().subscribe(&[&topic]);
            match res {
                Ok(_) => {
                    println!("kafka: subscribe to topic {}", &topic);
                    assert_sometimes!(true, "Consumer subscribed to topic", &json!({"result": format!("none")}));
                    break
                },
                Err(e) => {
                    eprintln!("kafka: Can't subscribe to specified topics: {}", e);
                    assert_sometimes!(false, "Consumer subscribed to topic", &json!({"error": format!("Can't subscribe to specified topics: {}", e)}));
                    sleep(Duration::from_secs(1)); // Avoid tight retry loop
                }
            }
        }
    }

    async fn safe_get_consumed(&mut self) -> Result<(BankData, NaiveDateTime)> {
        let message = self.consumer.as_ref().unwrap().recv().await
            .map_err(|e| {
                assert_sometimes!(false, "Consumer consumed data", &json!({ "error": format!("{:?}", e) }));
                e
            })?;

        assert_sometimes!(true, "Consumer consumed data", &json!({ "value": format!("{:?}", message) }));

        let payload_str = match message.payload_view::<str>() {
            Some(Ok(s)) => {
                println!("kafka: consumed message has correct string decoding");
                assert_sometimes!(true, "Consumer's consumed message has correct string decoding", &json!({ "value": s }));
                s
            },
            Some(Err(e)) => {
                eprintln!("kafka: consumed message has incorrect string decoding");
                assert_sometimes!(false, "Consumer's consumed message failed to decode payload", &json!({ "error": format!("{:?}", e) }));
                return Err(e.into());
            },
            None => {
                eprintln!("kafka: consumed message has no payload");
                assert_sometimes!(false, "Consumer's consumed message payload is None", &json!({ "error": "none" }));
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Kafka payload is None").into());
            }
        };
        
        match self.kind {
            KafkaConsumers::ExactlyOncePassThroughConsumer => {
                self.consumer.as_ref().unwrap().commit_message(&message, CommitMode::Sync)
                    .map_err(|e| {
                        println!("kafka: failed to commit message offset");
                        assert_sometimes!(false, "Consumer failed to commit offset", &json!({ "error": format!("{:?}", e) }));
                        e
                    })?;
            },
            _ => {
                println!("kafka: not committing, letting auto commit take care of it");
                assert_sometimes!(true, "Consumer failed to commit offset", &json!({ "error": "none" }));
            } 
        }
        

        let current_timestamp = chrono::Utc::now().naive_utc(); 

        let b_d: BankData = serde_json::from_str(payload_str.trim())
            .map_err(|e| {
                println!("kafka: consumed message failed to deserialize: {:?}", e);
                assert_sometimes!(false, "Consumer's consumed message failed to deserialize to BankData", &json!({ "error": format!("{:?}", e) }));
                anyhow::Error::from(e)
            })?;

        println!("kafka: consumed message deserializable to BankData: {:?}", b_d);
        assert_sometimes!(true, "Consumer's consumed message deserialized to BankData", &json!({ "result": format!("{:?}", b_d) }));

        Ok((b_d, current_timestamp))
    } 
}


fn create_router(state: Arc<AppState>) -> Router {
    let mut r = Router::new();
    for kafka_mode in KafkaConsumers::iter() {
        r = r.nest(
            kafka_mode.get_route(),
            create_nested_router(state.clone(), kafka_mode)
        )
    }
    r
}

fn create_nested_router(state: Arc<AppState>, mode: KafkaConsumers) -> Router{
     Router::new()
        .route("/:topic/:num_records", post(handle))
        .with_state(state) // Attach state to the router
        .layer(middleware::from_fn(move |req, next| {
            add_context_middleware(req, next, mode.clone())
        }))
       
}

async fn add_context_middleware(
    mut req: axum::http::Request<Body>,
    next: Next,
    context: KafkaConsumers, // Pass context explicitly
) -> Response {
    println!("middleware: adding context to request");
    req.extensions_mut().insert(context);
    // Should really put this in timeout to force this to stop holding the lock in handle
    // Right now we are just relying on multiple producers to fire to save us
    match tokio::spawn(next.run(req)).await {
        Ok(resp) => resp,
        Err(e) => {
            eprintln!("middleware: task failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn handle(
    Path((topic, num_records)): Path<(String, i64)>,
    State(state): State<Arc<AppState>>,
    Extension(kafka_mode): Extension<KafkaConsumers>
) {
    let brokers = &state.brokers;
    println!("kafka: waiting for lock, previous thred needs to let go");
    let mut consumers = state.consumers.lock().await;
    let consumer = consumers
        .entry(kafka_mode.get_route().to_string())
        .or_insert_with(|| {
            let mut kc = KafkaConsumer {kind: kafka_mode.clone(), consumer: None};
            kc.create_config(brokers);
            kc
        });
     
    let mut pg_client_guard = state.pg_client.lock().await; 
    if pg_client_guard.is_none() {
        *pg_client_guard = Some(
            Postgres::new("host=state-tracker user=u password=p dbname=d ")
            .await
            .map_err(|e| {
                eprintln!("postgres: Failed to initialize Postgres {:?}", e);
                e
            })
            .unwrap()
        );
        assert_reachable!("Consumer created connection to state-tracker", &json!({}));
    }

    let mut ddb_client_guard = state.ddb_client.lock().await;
    if ddb_client_guard.is_none() {
        let ddb = DynamoDb::new().await.unwrap();
        ddb.safe_create_table().await;
        *ddb_client_guard = Some( ddb );
        assert_reachable!("Consumer created connection to dynamodb and created tables", &json!({}));
    }


    if let Some(pg_client) = &*pg_client_guard {
        if let Some(ddb_client) = &*ddb_client_guard {
            consumer.safe_subscribe(&topic);
            let mut count = 0;
            while count < num_records {
                let b_d_data = consumer.safe_get_consumed().await;  
                match b_d_data {
                    Ok(b_d_data) => {
                        //b_d_data= temp(b_d_data);
                        pg_client.safe_write_consumed(&b_d_data, &kafka_mode).await;
                        ddb_client.safe_transaction(b_d_data.0).await;
                        //db_client.safe_transaction(b_d_data).await;
                    }
                    Err(e) => {
                        eprintln!("kafka: could not consume data error: {:?}, retrying ...", e);
                        sleep(Duration::from_secs(1)); // Avoid tight retry loop
                    }
                }
                count += 1; 
            }
        }
        
    }

}


#[derive(Clone)]
struct AppState {
    consumers: Arc<Mutex<HashMap<String, KafkaConsumer>>>,
    brokers: Vec<&'static str>,
    pg_client: Arc<Mutex<Option<Postgres>>>,
    ddb_client: Arc<Mutex<Option<DynamoDb>>> 
    // faker_endpoints: Vec<&'static str>
}

#[tokio::main]
async fn main() {
    antithesis_init();
    // env_logger::init();

    let brokers = vec!["kafka-3:9092", "kafka-2:9092", "kafka-1:9092"];
    let pg_client = Arc::new(Mutex::new(None));
    let ddb_client = Arc::new(Mutex::new(None));
    let consumers: Arc<Mutex<HashMap<String, KafkaConsumer>>> = Arc::new(Mutex::new(HashMap::new()));
    let state = AppState { consumers, brokers, pg_client, ddb_client };
    // let app = create_nested_router(state.into());
    let app = create_router(state.into());
    println!("Starting Consumer");
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running at http:{:?}", listener);
    
    axum::serve(listener, app).await.unwrap(); 
}
