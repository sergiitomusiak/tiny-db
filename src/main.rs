use tiny_db::{Options, Store};

#[tokio::main]
async fn main() {
    let store = Store::open("./db/", Options::default()).await.unwrap();
    for i in 0..1_000_000 {
        store.insert(i % 1000, format!("value_{i}")).await.unwrap();
    }
    let mut found = 0;
    let mut total_len: usize = 0;
    for i in 0..2_000_000 {
        let value: Option<String> = store.get(&i).await.unwrap();
        if let Some(value) = value {
            found += 1;
            total_len += value.len();
        }
    }
    println!("found = {found}, total len = {total_len}");
    store.force_compaction().await.expect("force compaction");
}
