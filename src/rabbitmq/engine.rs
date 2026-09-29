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
    dead_letter_exchange: Option<ShortString>,
    dead_letter_routing_key: Option<ShortString>,
    message_ttl: Option<u64>,
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
    pub routing_key: ShortString,
    pub data: Vec<u8>,
    pub expiration: Option<std::time::Instant>,
    pub redelivered: bool,
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

    pub fn declare_queue(
        &self,
        name: ShortString,
        arguments: amq_protocol::types::FieldTable,
    ) -> ShortString {
        let mut state = self.state.lock().unwrap();
        let q_name = if name.as_str().is_empty() {
            ShortString::from(format!("amq.gen-{}", fastrand::u32(..)))
        } else {
            name
        };

        let mut dead_letter_exchange = None;
        let mut dead_letter_routing_key = None;
        let mut message_ttl = None;
        let map = arguments.inner();
        if let Some(amq_protocol::types::AMQPValue::LongString(s)) =
            map.get("x-dead-letter-exchange")
        {
            dead_letter_exchange = Some(std::str::from_utf8(s.as_bytes()).unwrap().into());
        }
        if let Some(amq_protocol::types::AMQPValue::LongString(s)) =
            map.get("x-dead-letter-routing-key")
        {
            dead_letter_routing_key = Some(std::str::from_utf8(s.as_bytes()).unwrap().into());
        }
        if let Some(amq_protocol::types::AMQPValue::LongUInt(v)) = map.get("x-message-ttl") {
            message_ttl = Some(*v as u64);
        } else if let Some(amq_protocol::types::AMQPValue::LongLongInt(v)) =
            map.get("x-message-ttl")
        {
            message_ttl = Some(std::cmp::max(0, *v) as u64);
        }

        state.queues.entry(q_name.clone()).or_insert_with(|| Queue {
            name: q_name.clone(),
            messages: VecDeque::new(),
            dead_letter_exchange,
            dead_letter_routing_key,
            message_ttl,
        });
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

    fn publish_internal(
        &self,
        state: &mut State,
        exchange: ShortString,
        routing_key: ShortString,
        msg: Message,
    ) {
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
                let mut m = msg.clone();
                if let Some(ttl) = queue.message_ttl {
                    let q_exp = std::time::Instant::now() + std::time::Duration::from_millis(ttl);
                    if let Some(msg_exp) = m.expiration {
                        m.expiration = Some(std::cmp::min(msg_exp, q_exp));
                    } else {
                        m.expiration = Some(q_exp);
                    }
                }
                queue.messages.push_back(m);
            }
        }
    }

    pub fn publish(&self, exchange: ShortString, routing_key: ShortString, msg: Message) {
        let mut state = self.state.lock().unwrap();
        self.publish_internal(&mut state, exchange, routing_key, msg);
    }

    fn dead_letter_internal(&self, state: &mut State, queue_name: &ShortString, msg: Message) {
        let (dlx, dlrk) = if let Some(queue) = state.queues.get(queue_name) {
            if let Some(dlx) = &queue.dead_letter_exchange {
                (dlx.clone(), queue.dead_letter_routing_key.clone())
            } else {
                return;
            }
        } else {
            return;
        };

        let rk = dlrk.unwrap_or_else(|| msg.routing_key.clone());
        let mut new_msg = msg;
        new_msg.expiration = None; // Reset TTL for dead-lettered message
        self.publish_internal(state, dlx, rk, new_msg);
    }

    pub fn dead_letter(&self, queue_name: &ShortString, msg: Message) {
        let mut state = self.state.lock().unwrap();
        self.dead_letter_internal(&mut state, queue_name, msg);
    }

    pub fn requeue(&self, queue_name: &ShortString, mut msg: Message) {
        let mut state = self.state.lock().unwrap();
        if let Some(q) = state.queues.get_mut(queue_name) {
            msg.redelivered = true;
            q.messages.push_front(msg);
        }
    }

    pub fn basic_get(&self, queue: &ShortString) -> Option<Message> {
        let mut state = self.state.lock().unwrap();

        let mut expired_msgs = Vec::new();
        let res = if let Some(q) = state.queues.get_mut(queue) {
            let mut got = None;
            while let Some(msg) = q.messages.pop_front() {
                if let Some(exp) = msg.expiration
                    && std::time::Instant::now() >= exp
                {
                    expired_msgs.push(msg);
                    continue;
                }
                got = Some(msg);
                break;
            }
            got
        } else {
            None
        };

        for msg in expired_msgs {
            self.dead_letter_internal(&mut state, queue, msg);
        }

        res
    }
}
