async fn second_recv(wss: &mut switch_ws::net::Wss) {
    let m = wss.recv().await.unwrap().unwrap();
    let _ = wss.recv().await;
    let _ = m.bytes();
}

fn main() {}
