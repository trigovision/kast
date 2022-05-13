use kast::{
    context::Context, encoders::JsonEncoder, input::Input, processor::Processor,
    state_store::InMemoryStateStore,
};
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

async fn handle_clicks_stateful(state: &mut (), ctx: &mut Context<ClicksPerUser>, _click: Click) {
    let mut clicks_per_user = match ctx.get_state() {
        Some(state) => state.clone(),
        None => ClicksPerUser { clicks: 0 },
    };
    clicks_per_user.clicks += 1;
    println!("Hi {:?}", state);
    // state.shit *= 2;
    ctx.set_state(Some(clicks_per_user))
}

async fn emit_clicks_stateless(_state: &mut (), ctx: &mut Context<ClicksPerUser>, click: Click2) {
    let key = ctx.key().to_string();
    for _ in 0..click.clicks {
        ctx.emit("c1", &key, &Click {})
    }
}

//TODO: Support outputs
//TODO: Support different state stores which arent hashmaps
#[tokio::main]
async fn main() {
    let settings = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("partitioner", "murmur2")
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
            Input::new("c1".to_string(), json_encoder, handle_clicks_stateful),
            Input::new("c2".to_string(), JsonEncoder::new(), emit_clicks_stateless),
        ],
        InMemoryStateStore::new,
        || (),
    );

    p.run().await
}

#[cfg(test)]
mod tests {
    use futures::future::join_all;
    use kast::{
        encoders::JsonEncoder, input::Input, state_store::InMemoryStateStore,
        test_utils::TestProcessor,
    };
    use rstest::rstest;
    use serde::Serialize;

    use crate::{emit_clicks_stateless, handle_clicks_stateful, json_encoder, Click, Click2};

    fn json_decoder<T: Serialize>(data: &T) -> Vec<u8> {
        serde_json::to_vec(&data).unwrap()
    }

    #[rstest]
    #[tokio::test]
    async fn test_single_store() {
        let mut p = TestProcessor::new(
            vec![
                Input::new("c1".to_string(), json_encoder, handle_clicks_stateful),
                Input::new("c2".to_string(), JsonEncoder::new(), emit_clicks_stateless),
            ],
            InMemoryStateStore::new,
            || (),
        );

        let mut in1 = p.input("c1".to_string(), json_decoder);
        let mut in2 = p.input("c2".to_string(), json_decoder);

        let task = tokio::spawn(async move {
            p.run().await;
        });

        in1.send("a".to_string(), &Click {}).await.unwrap();
        in2.send("a".to_string(), &Click2 { clicks: 10 })
            .await
            .unwrap();

        join_all(vec![task]).await;
    }
}
