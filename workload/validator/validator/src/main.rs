use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};

use std::fmt::Write;

use log::{info, error, debug, warn};
use env_logger;

use reqwest;

use tokio::sync::Mutex;
use std::sync::Arc;
use std::collections::HashMap;
// use std::error::Error;

use std::net::SocketAddr;

use serde::{Serialize, Deserialize};
use serde_json::Value;
use serde_json::json;
use antithesis_sdk::prelude::*;
use serde_postgres::de::from_row;

use strum::IntoEnumIterator;
use strum_macros::EnumIter;
use tokio_postgres::{Client, NoTls, Error, Row};

use postgres_types::{Type, IsNull};
use tokio::time::sleep;
use clap::{Arg, Command, ValueEnum};


// 1. Everything that has been produced has been consumed
//      sql statement to check everything with a producer timestamp has a consumer timestamp
//      make this check 30s long
// 2. Consumption order == Faker Production order: 
//      this can be checked by ensuring the ids of all consumer 
//      timestamps is in monotonic incremental order
// 3. Consumed count == 1 && Producer type == "/exactly_once":
//      trivial sql statement

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BankData {
    id: i32,
    data: String, 
    produced_timestamp: Option<SystemTime>,
    producer_type: Option<String>,
    consumed_timestamp: Option<SystemTime>,
    consumer_type: Option<String>,
    consumed_count: i32,
    topic: String
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
            topic: row.get("topic")
        }
    }
}


#[derive(Debug)]
struct Postgres {
    client: Client
}

impl Postgres {
    pub async fn new(connection_string: &str) -> Result<Self, Error> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(connection); // Spawn the connection as a background task
        Ok(Postgres { client })
    }

    async fn get_produced_data(&self) -> Result<Vec<BankData>, Error> {
        let query = "
            SELECT * 
            FROM producer 
            WHERE produced_timestamp IS NOT NULL 
            ORDER BY id ASC 
        ";

        let rows = self.client
            .query(query, &[])
            .await
            .map_err(|e| {
                error!("Failed to perform query: {}", e);
                //assert_sometimes!(false, "")
                e
            })?;

        let accounts: Vec<BankData> = rows
            .iter()
            .map(BankData::from_row)
            .collect();

        Ok(accounts)
    }

    async fn get_consumed_data(&self) -> Result<Vec<BankData>, Error> {
        let query = "
            SELECT * 
            FROM producer 
            WHERE consumed_timestamp IS NOT NULL 
            ORDER BY id ASC 
        ";

        let rows = self.client
            .query(query, &[])
            .await
            .map_err(|e| {
                error!("Failed to perform query: {}", e);
                e
            })?;

        let accounts: Vec<BankData> = rows
            .iter()
            .map(BankData::from_row)
            .collect();
        
        Ok(accounts)
    }

    // Exactly-once rows that were both produced and consumed, in the order the
    // consumer first consumed them. produced_seq and consumed_seq come from one
    // Postgres sequence, so they order events without relying on clocks.
    async fn get_exactly_once_consumption_order(&self) -> Result<Vec<(i32, i64, i64)>, Error> {
        let query = "
            SELECT id, produced_seq, consumed_seq
            FROM producer
            WHERE producer_type LIKE '%exactly_once%'
              AND produced_seq IS NOT NULL
              AND consumed_seq IS NOT NULL
            ORDER BY consumed_seq ASC
        ";

        let rows = self.client
            .query(query, &[])
            .await
            .map_err(|e| {
                error!("Failed to perform query: {}", e);
                e
            })?;

        Ok(rows.iter().map(|r| (r.get(0), r.get(1), r.get(2))).collect())
    }

    async fn check_exactly_once_producer_guarantee(&self) -> Result<Vec<BankData>, Error> {
        let query = "
            SELECT *
            FROM producer
            WHERE consumed_count > 1 AND producer_type LIKE '%exactly_once%'
        ";

        let rows = self.client
            .query(query, &[])
            .await
            .map_err(|e| {
                error!("Failed to perform query: {}", e);
                e
            })?;
        
        let accounts: Vec<BankData> = rows
            .iter()
            .map(BankData::from_row)
            .collect();
        
        Ok(accounts)
    }
}

#[derive(ValueEnum, Clone, Debug)]
enum Tests {
    ExactlyOnceGuarantee,
    ConsumptionOrder,
    ProducerConsumerMatches
}

// Every row consumed after another must also have been produced after it.
// Returns each (earlier, later) pair of consecutive consumptions that breaks that.
fn find_out_of_order_consumption(rows: &[(i32, i64, i64)]) -> Vec<Value> {
    rows.windows(2)
        .filter(|pair| pair[1].1 < pair[0].1)
        .map(|pair| json!({
            "consumed_first": {"id": pair[0].0, "produced_seq": pair[0].1, "consumed_seq": pair[0].2},
            "consumed_next": {"id": pair[1].0, "produced_seq": pair[1].1, "consumed_seq": pair[1].2},
        }))
        .collect()
}

fn find_produced_data_thats_not_consumed_yet(b_as: &[BankData]) -> Vec<BankData> {
    b_as.iter()
        .filter_map(|b_a| {
            if b_a.produced_timestamp.is_some() && b_a.consumed_timestamp.is_some() {
                None
            } else {
                Some(b_a.clone())
            }
        })
        .collect() 
}

async fn run_check_for_30s(pg_client: Postgres) -> Option<Vec<BankData>> {
    let start_time = Instant::now();
    let duration = Duration::from_secs(30);
    let mut last_result = None;

    while Instant::now().duration_since(start_time) < duration {
        match pg_client.get_produced_data().await {
            Ok(b_as) => {
                info!("Produced data length {:?}", b_as.len());
                last_result = Some(find_produced_data_thats_not_consumed_yet(&b_as));
                //info!("Produced data thats not consumed yet {:?}", last_result);
            }
            Err(err) => {
                error!("Failed to get produced data: {}", err);
            }
        }
        sleep(Duration::from_millis(5000)).await;
    }
    
    info!("Finished checking after 30 seconds.");
    last_result
}

#[tokio::main]
async fn main() {
    antithesis_init();
    env_logger::init();

    if let Ok(pg_client) = Postgres::new("host=state-tracker user=u password=p dbname=d ").await {
        info!("Postgres client initialized successfully.");
        let matches = Command::new("Validator CLI")
            .version("1.0")
            .author("ADawg")
            .about("You can use this to launch specific validation logic around Kafka Consumers and Producer")
            .arg(
                Arg::new("test")
                    .short('t')
                    .long("test")
                    .value_parser(clap::builder::EnumValueParser::<Tests>::new())
                    .required(true)
                    .help("Specifies the test to perform: ExactlyOnceGuarantee or ConsumptionOrder or ProducerConsumerMatches"),
            )
            .get_matches();

        let test: Tests = matches.get_one::<Tests>("test").unwrap().to_owned();
        match test {
            Tests::ExactlyOnceGuarantee => {
                match pg_client.check_exactly_once_producer_guarantee().await {
                    Ok(b_as) => {
                        assert_always!(b_as.len() == 0, "Instances of exactly_once_producer produces more than once", &json!({"result": b_as}));
                    },
                    Err(err) => {
                        error!("Failed to check exactly_once guarantee: {}", err);
                    }
                }
            }
            Tests::ConsumptionOrder => {
                // Kafka orders messages within a partition, and bankEO has one
                // partition and an idempotent, transactional producer whose
                // requests the producer service handles one at a time. So bankEO
                // must be consumed in the order it was produced. The at-least-once
                // topic makes no such promise: a send that times out may still be
                // delivered, and the producer service sends that row again later.
                match pg_client.get_exactly_once_consumption_order().await {
                    Ok(rows) => {
                        let out_of_order = find_out_of_order_consumption(&rows);
                        assert_always!(
                            out_of_order.is_empty(),
                            "Exactly-once topic is consumed in the order it was produced",
                            &json!({"checked": rows.len(), "out_of_order": out_of_order})
                        )
                    },
                    Err(err) => {
                        error!("Failed to get exactly-once consumption order: {}", err);
                    }
                }
            }
            Tests::ProducerConsumerMatches => {
                match run_check_for_30s(pg_client).await {
                    Some(b_as) => {
                        // On the test composer eventually script
                        // Call the consumer with a large number of consumption before calling the validator
                        info!("Check completed");
                        assert_always!(b_as.len() == 0, "Produced data matches consumed data after 30s of not producing and only consuming", &json!({"result": b_as}))
                    },
                    None => {}
                }
            }
        }
    } else {
        error!("Failed to initialize Postgres client.");
    }
    //let action = matches.value_of("action").unwrap();
    // println!("Hello, world!");
}
