use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct SqlRequest {
    pub sql: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SqlResponse {
    pub success: bool,
    pub result: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_ms: Option<f64>,
}

impl SqlResponse {
    pub fn success(result: String, row_count: usize, elapsed_ms: f64) -> Self {
        Self {
            success: true,
            result,
            row_count: Some(row_count),
            elapsed_ms: Some(elapsed_ms),
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            result: message,
            row_count: None,
            elapsed_ms: None,
        }
    }

    pub fn _ok(message: String) -> Self {
        Self {
            success: true,
            result: message,
            row_count: None,
            elapsed_ms: None,
        }
    }
}
