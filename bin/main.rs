use kast::{Context, Input, Processor};
use rdkafka::ClientConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct Click {}

#[derive(Debug, Clone, Deserialize)]
struct ClicksPerUser {
    clicks: u32,
}

fn print_message(_ctx: Context, click: &Click) {
    println!("Hi {:?}", click)
}

fn print_message2(_ctx: Context, click: &Click) {
    println!("Hi2 {:?}", click)
}

#[tokio::main]
async fn main() {
    let settings = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "keypoints2")
        .set("heartbeat.interval.ms", "250")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "largest")
        // This requires broker's config 'group.min.session.timeout.ms' to be less than 1000 (by default its 6000)
        .set("session.timeout.ms", "1000")
        .set("max.poll.interval.ms", "1500")
        .clone();

    // let clicks_input = ;
    let p = Processor::new(
        settings,
        vec![
            Box::new(Input::new("c1".to_string(), print_message)),
            Box::new(Input::new("c2".to_string(), print_message2)),
        ],
    );

    p.run().await
}
