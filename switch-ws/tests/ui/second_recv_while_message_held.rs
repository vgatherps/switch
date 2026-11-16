async fn second_recv(wss: &mut switch_ws::switch_net::Wss) {
    let m = wss.recv().await.unwrap().unwrap();
    let _ = wss.recv().await;
    let _ = m.bytes();
}

fn main() {}
