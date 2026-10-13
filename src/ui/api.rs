//! Typed view of the control API (see docs/control-api.md).

use super::http::{ApiError, request};
use serde::Deserialize;
use std::path::Path;

#[derive(Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Service {
    pub name: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub connect: String,
    #[serde(default)]
    pub data_bytes: Option<u64>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Deserialize, Clone, Debug, Default, PartialEq)]
pub struct Status {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub pid: u32,
    #[serde(default)]
    pub data_dir: String,
    #[serde(default)]
    pub uptime_ms: u64,
    #[serde(default)]
    pub rss_bytes: Option<u64>,
    #[serde(default)]
    pub services: Vec<Service>,
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct Event {
    pub seq: u64,
    #[serde(default)]
    pub ts_ms: u64,
    #[serde(default)]
    pub service: String,
    #[serde(default)]
    pub text: String,
}

#[derive(Deserialize, Debug)]
pub struct Activity {
    pub next: u64,
    #[serde(default)]
    pub events: Vec<Event>,
}

#[derive(Deserialize)]
struct ControlFile {
    port: u16,
    token: String,
}

#[derive(Clone, Debug)]
pub struct Client {
    pub port: u16,
    pub token: String,
}

impl Client {
    /// Read `<data_dir>/noida.control.json`; None when absent or unreadable.
    pub fn from_data_dir(dir: &Path) -> Option<Client> {
        let text = std::fs::read_to_string(dir.join("noida.control.json")).ok()?;
        let f: ControlFile = serde_json::from_str(&text).ok()?;
        Some(Client { port: f.port, token: f.token })
    }

    fn call<T: for<'de> Deserialize<'de>>(&self, method: &str, path: &str) -> Result<T, ApiError> {
        let v = request(self.port, &self.token, method, path)?;
        serde_json::from_value(v).map_err(|e| ApiError::Down(format!("unexpected reply: {e}")))
    }

    pub fn status(&self) -> Result<Status, ApiError> {
        self.call("GET", "/v1/status")
    }

    pub fn activity(&self, since: u64) -> Result<Activity, ApiError> {
        self.call("GET", &format!("/v1/activity?since={since}&limit=200"))
    }

    /// `verb` is start, stop, restart or reset.
    pub fn service_action(&self, name: &str, verb: &str) -> Result<Service, ApiError> {
        #[derive(Deserialize)]
        struct R {
            service: Service,
        }
        let r: R = self.call("POST", &format!("/v1/services/{name}/{verb}"))?;
        Ok(r.service)
    }

    pub fn shutdown(&self) -> Result<(), ApiError> {
        request(self.port, &self.token, "POST", "/v1/shutdown").map(|_| ())
    }
}
