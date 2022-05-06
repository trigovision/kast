use kast::{Context, Input, Processor};
use rdkafka::ClientConfig;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct Click {}

#[derive(Debug, Clone, Deserialize)]
struct Click2 {
    clicks: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct ClicksPerUser {
    clicks: u32,
}

fn print_message(ctx: &mut Context<ClicksPerUser>, _click: &Click) {
    let clicks_per_user = match ctx.get_state() {
        Some(mut state) => {
            state.clicks += 1;
            state
        }
        None => ClicksPerUser { clicks: 0 },
    };
    println!("Hi {:?}", clicks_per_user);
    ctx.set_state(Some(clicks_per_user))
}

fn print_message2(ctx: &mut Context<ClicksPerUser>, click: &Click2) {
    let clicks_per_user = match ctx.get_state() {
        Some(mut state) => {
            state.clicks += click.clicks;
            state
        }
        None => ClicksPerUser {
            clicks: click.clicks,
        },
    };
    println!("Hi2 {:?}", clicks_per_user);
    ctx.set_state(Some(clicks_per_user))
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
