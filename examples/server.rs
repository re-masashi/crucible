use axum::{
    routing::{get, post},
    Router,
};
use crucible::{Store, Term};
use std::net::SocketAddr;
use std::sync::Arc;

type SharedStore = Arc<Store>;

#[tokio::main]
async fn main() {
    let store = Arc::new(Store::new_default(Some("crucible".to_string())));

    let app = Router::new()
        .route("/get/{key}", get(get_key))
        .route("/set", post(set_key))
        .with_state(store);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_key(
    axum::extract::Path(key): axum::extract::Path<String>,
    axum::extract::State(store): axum::extract::State<SharedStore>,
) -> axum::Json<Option<String>> {
    let result = store.get(&key);
    axum::Json(result.ok().map(|t| format!("{:?}", t)))
}

async fn set_key(
    axum::extract::State(store): axum::extract::State<SharedStore>,
    axum::Json(payload): axum::Json<SetRequest>,
) -> axum::Json<&'static str> {
    store
        .insert(&payload.key, Term::Int(payload.value))
        .unwrap();
    axum::Json("OK")
}

#[derive(serde::Deserialize)]
struct SetRequest {
    key: String,
    value: i64,
}
