//TODO: Support decoder per output?

#[derive(Clone)]
pub struct Output {
    topic: String,
}

impl Output {
    pub fn new(topic: String) -> Self {
        Output { topic }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}
