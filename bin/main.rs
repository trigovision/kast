use std::{collections::HashMap, sync::Arc};

use kast::{
    context::Context, encoders::JsonDecoder, input::Input, output::Output, processor::Processor,
    processor_helper::KafkaProcessorHelper, state_store::StateStore,
};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

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

async fn handle_click<TStore>(ctx: &mut Context<TStore, ClicksPerUser>, _click: Click) -> Option<ClicksPerUser>
where
    TStore: StateStore<ClicksPerUser>,
{
    let mut clicks_per_user = match ctx.get_state() {
        Some(state) => state.clone(),
        None => ClicksPerUser { clicks: 0 },
    };

    clicks_per_user.clicks += 1;
    println!("{:?}, {:?}", ctx.key(), clicks_per_user);

    Some(clicks_per_user)
}

async fn re_emit_clicks<TStore>(ctx: &mut Context<TStore, ClicksPerUser>, click: Click2) -> Option<ClicksPerUser>
where
    TStore: StateStore<ClicksPerUser>,
{
    let key = ctx.key().to_string();
    for _ in 0..click.clicks {
        ctx.emit("c1", &key, &Click {})
    }

    None
}

//TODO: Support outputs
//TODO: Support different state stores which arent hashmaps
#[tokio::main]
async fn main() {
    let settings = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("partitioner", "murmur2")
        .set("group.id", "keypoints444")
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

    let p = Processor::new(
        "clicks",
        KafkaProcessorHelper::new(settings),
        vec![
            Input::new("c1", JsonDecoder::new(), handle_click),
            Input::new("c2", JsonDecoder::new(), re_emit_clicks),
        ],
        vec![Output::new("c1")],
        || Arc::new(Mutex::new(HashMap::<String, ClicksPerUser>::new())),
        || (),
    );

    p.run_forever().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::{handle_click, re_emit_clicks, Click, Click2, ClicksPerUser};
    use kast::{
        encoders::{JsonDecoder, JsonEncoder},
        test_utils::TestsProcessorHelper,
    };
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_single_store() {
        let mut t = TestsProcessorHelper::new(vec!["c1", "c2", "c3"]);
        let state_store = Arc::new(Mutex::new(HashMap::new()));
        let state_store_clone = state_store.clone();
        let mut in1 = t.input("c1", JsonEncoder::new());
        let mut in2 = t.input("c2", JsonEncoder::new());

        let p = Processor::new(
            "clicks",
            t,
            vec![
                Input::new("c1", JsonDecoder::new(), handle_click),
                Input::new("c2", JsonDecoder::new(), re_emit_clicks),
            ],
            vec![Output::new("c1")],
            move || state_store_clone,
            || (),
        );

        for _i in 0..100000 {
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

        p.run_forever().await;

        let lock = state_store.lock().await;

        let final_state: &ClicksPerUser = lock.get("clicks::a").unwrap();
        assert_eq!(final_state.clicks, 110000);

        let final_state: &ClicksPerUser = lock.get("clicks::b").unwrap();
        assert_eq!(final_state.clicks, 10000);

        let final_state: &ClicksPerUser = lock.get("clicks::c").unwrap();
        assert_eq!(final_state.clicks, 1);
    }
}
