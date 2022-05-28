use std::collections::HashMap;

use kast::{
    context::Context, encoders::JsonEncoder, input::Input, output::Output, processor::Processor,
    processor_helper::KafkaProcessorHelper,
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

async fn handle_clicks_stateless(ctx: &mut Context<ClicksPerUser>, _click: Click) {
    let mut clicks_per_user = match ctx.get_state() {
        Some(state) => state.clone(),
        None => ClicksPerUser { clicks: 0 },
    };
    clicks_per_user.clicks += 1;
    ctx.set_state(Some(clicks_per_user))
}

async fn emit_clicks_stateful(ctx: &mut Context<ClicksPerUser>, click: Click2) {
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
    let mut p = Processor::new(
        KafkaProcessorHelper::new(settings),
        vec![
            Input::new("c1".to_string(), json_encoder, handle_clicks_stateless),
            Input::new("c2".to_string(), JsonEncoder::new(), emit_clicks_stateful),
        ],
        vec![Output::new("c1".to_string())],
        HashMap::<String, ClicksPerUser>::new,
        || (),
    );

    p.start().await;
    p.join().await
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{
        emit_clicks_stateful, handle_clicks_stateless, json_encoder, Click, Click2, ClicksPerUser,
    };
    use kast::{
        encoders::{JsonDecoder, JsonEncoder},
        input::Input,
        output::Output,
        processor::Processor,
        state_store::StateStore,
        test_utils::TestsProcessorHelper,
    };
    use rstest::rstest;
    use tokio::sync::RwLock;

    #[rstest]
    #[tokio::test]
    async fn test_single_store() {
        let mut t = TestsProcessorHelper::new(vec!["c1", "c2", "c3"]);
        let state_store = Arc::new(RwLock::new(HashMap::new()));
        let state_store_clone = state_store.clone();
        let mut in1 = t.input("c1".to_string(), JsonDecoder::new());
        let mut in2 = t.input("c2".to_string(), JsonDecoder::new());

        let mut p = Processor::new(
            t,
            vec![
                Input::new("c1".to_string(), json_encoder, handle_clicks_stateless),
                Input::new("c2".to_string(), JsonEncoder::new(), emit_clicks_stateful),
            ],
            vec![Output::new("c1".to_string())],
            move || state_store_clone.clone(),
            || (),
        );

        p.start().await;

        for _i in 0..1000 {
            in1.send("a".to_string(), &Click {}).await.unwrap();
        }

        in2.send("a".to_string(), &Click2 { clicks: 10000 })
            .await
            .unwrap();

        in2.send("b".to_string(), &Click2 { clicks: 10000 })
            .await
            .unwrap();

        in2.send("c".to_string(), &Click2 { clicks: 1 })
            .await
            .unwrap();

        p.join().await;

        let final_state: ClicksPerUser = state_store.get("a").await.unwrap();
        assert_eq!(final_state.clicks, 11000);

        let final_state: ClicksPerUser = state_store.get("b").await.unwrap();
        assert_eq!(final_state.clicks, 10000);

        let final_state: ClicksPerUser = state_store.get("c").await.unwrap();
        assert_eq!(final_state.clicks, 1);
    }
}
