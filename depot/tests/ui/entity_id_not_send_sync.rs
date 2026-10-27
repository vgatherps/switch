depot::define_static_ecs!(EntityIdThreadPool, 64);

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}

fn main() {
    assert_send::<EntityIdThreadPool>();
    assert_sync::<EntityIdThreadPool>();
    assert_send::<depot::EntityId<EntityIdThreadPool>>();
    assert_sync::<depot::EntityId<EntityIdThreadPool>>();
}
