use kast::{encoders::JsonEncoder, state_store::InMemoryStateStore, Context, Input, Processor};
use rdkafka::ClientConfig;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

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

fn json_encoder<T: DeserializeOwned>(data: Option<&[u8]>) -> T {
    serde_json::from_slice(data.expect("empty message")).unwrap()
}

fn handle_clicks_stateful(ctx: &mut Context<ClicksPerUser>, _click: &Click) {
    let mut clicks_per_user = match ctx.get_state() {
        Some(state) => state,
        None => ClicksPerUser { clicks: 0 },
    };
    clicks_per_user.clicks += 1;
    println!("Hi {:?}", clicks_per_user);
    ctx.set_state(Some(clicks_per_user))
}

fn emit_clicks_stateless(ctx: &mut Context<ClicksPerUser>, click: &Click2) {
    for _ in 0..click.clicks {
        ctx.emit("c1", ctx.key(), &Click {})
    }
}

//TODO: Support outputs
//TODO: Support different state stores which arent hashmaps
#[tokio::main]
async fn main() {
    let settings = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "keypoints")
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
            Box::new(Input::new(
                "c1".to_string(),
                json_encoder,
                handle_clicks_stateful,
            )),
            Box::new(Input::new(
                "c2".to_string(),
                JsonEncoder::new(),
                emit_clicks_stateless,
            )),
        ],
        InMemoryStateStore::new,
    );

    p.run().await
}
