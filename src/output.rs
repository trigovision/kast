//TODO: Support decoder per output?

#[derive(Clone)]
pub struct Output {
    topic: String,
}

impl Output {
    pub fn new(topic: &str) -> Self {
        Output { topic: topic.to_string() }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}
