#[derive(serde::Serialize)]
pub struct ValueResponse {
    pub key: String,
    pub value: String,
}

#[derive(serde::Deserialize)]
pub struct SetRequest {
    pub value: String,
}
