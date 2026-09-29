use amq_protocol::types::ShortString;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct Engine {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    queues: HashMap<ShortString, Queue>,
    exchanges: HashMap<ShortString, Exchange>,
    bindings: Vec<Binding>,
}

struct Queue {
    #[allow(dead_code)]
    name: ShortString,
    messages: VecDeque<Message>,
}

struct Exchange {
    #[allow(dead_code)]
    name: ShortString,
    #[allow(dead_code)]
    kind: ShortString,
}

struct Binding {
    queue: ShortString,
    exchange: ShortString,
    routing_key: ShortString,
}

#[derive(Clone)]
pub struct Message {
    pub content_type: Option<ShortString>,
    pub data: Vec<u8>,
}

impl Engine {
    pub fn new() -> Self {
        let mut state =
            State { queues: HashMap::new(), exchanges: HashMap::new(), bindings: Vec::new() };
        state.exchanges.insert(
            "amq.direct".into(),
            Exchange { name: "amq.direct".into(), kind: "direct".into() },
        );
        state.exchanges.insert("".into(), Exchange { name: "".into(), kind: "direct".into() }); // default exchange
        Self { state: Arc::new(Mutex::new(state)) }
    }

    pub fn declare_queue(&self, name: ShortString) -> ShortString {
        let mut state = self.state.lock().unwrap();
        let q_name = if name.as_str().is_empty() {
            ShortString::from(format!("amq.gen-{}", fastrand::u32(..)))
        } else {
            name
        };
        state
            .queues
            .entry(q_name.clone())
            .or_insert_with(|| Queue { name: q_name.clone(), messages: VecDeque::new() });
        q_name
    }

    pub fn declare_exchange(&self, name: ShortString, kind: ShortString) {
        let mut state = self.state.lock().unwrap();
        state.exchanges.insert(name.clone(), Exchange { name, kind });
    }

    pub fn bind_queue(&self, queue: ShortString, exchange: ShortString, routing_key: ShortString) {
        let mut state = self.state.lock().unwrap();
        state.bindings.push(Binding { queue, exchange, routing_key });
    }

    pub fn publish(&self, exchange: ShortString, routing_key: ShortString, msg: Message) {
        let mut state = self.state.lock().unwrap();
        // naive routing
        let mut target_queues = Vec::new();
        if exchange.as_str().is_empty() {
            target_queues.push(routing_key);
        } else {
            for b in &state.bindings {
                if b.exchange == exchange && b.routing_key == routing_key {
                    target_queues.push(b.queue.clone());
                }
            }
        }
        for q in target_queues {
            if let Some(queue) = state.queues.get_mut(&q) {
                queue.messages.push_back(msg.clone());
            }
        }
    }

    pub fn basic_get(&self, queue: &ShortString) -> Option<Message> {
        let mut state = self.state.lock().unwrap();
        if let Some(q) = state.queues.get_mut(queue) { q.messages.pop_front() } else { None }
    }
}
