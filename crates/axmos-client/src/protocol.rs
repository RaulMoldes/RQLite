use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct SqlRequest {
    pub sql: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SqlResponse {
    pub result: String,
}
