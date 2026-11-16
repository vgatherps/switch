struct Server;

impl Server {
    #[switch_runtime::task]
    async fn run(&self) {}
}

fn main() {}
