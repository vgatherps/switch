async fn second_recv(wss: &mut relay::switchnet::Wss) {
    let m = wss.recv().await.unwrap().unwrap();
    let _ = wss.recv().await;
    let _ = m.bytes();
}

fn main() {}
