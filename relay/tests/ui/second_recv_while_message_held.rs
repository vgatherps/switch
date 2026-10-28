use relay::{Config, Raw, Role, RxFrame, WsRx};

struct F(Vec<u8>);

impl RxFrame for F {
    fn payload(&self) -> &[u8] {
        &self.0
    }
    fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
    fn rx_timestamp(&self) -> u64 {
        0
    }
}

fn main() {
    let mut rx: WsRx<Raw<F>> = WsRx::new(Raw::new(), Config::new(Role::Client));
    let m = rx.recv(&mut ()).unwrap().unwrap();
    let _ = rx.recv(&mut ());
    drop(m);
}
