use kast::{Context, InMemoryStateStore, Input, Processor};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Click {}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Click2 {
    clicks: u16,
}

#[derive(Debug, Clone, Deserialize)]
struct ClicksPerUser {
    clicks: u32,
}

fn print_message(ctx: &mut Context<ClicksPerUser>, _click: &Click) {
    let mut clicks_per_user = match ctx.get_state() {
        Some(state) => state,
        None => ClicksPerUser { clicks: 0 },
    };
    clicks_per_user.clicks += 1;
    println!("Hi {:?}", clicks_per_user);
    ctx.set_state(Some(clicks_per_user))
}

fn print_message2(ctx: &mut Context<ClicksPerUser>, click: &Click2) {
    let mut clicks_per_user = match ctx.get_state() {
        Some(state) => state,
        None => ClicksPerUser { clicks: 0 },
    };
    for _ in 0..click.clicks {
        ctx.emit("c1", "b", &Click {})
    }
    clicks_per_user.clicks += click.clicks as u32;
    println!("Hi2 {:?}", clicks_per_user);
    ctx.set_state(Some(clicks_per_user))
}

//TODO: Support outputs
//TODO: Support different state stores which arent hashmaps
#[tokio::main]
async fn main() {
    let settings = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "keypoints2")
        .set("heartbeat.interval.ms", "250")
        .set("enable.auto.commit", "true")
        // This is important so offset won't be store automatically
        // TODO: Enforce / verify / override this?
        .set("enable.auto.offset.store", "false")
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
        InMemoryStateStore::new,
    );

    p.run().await
}
