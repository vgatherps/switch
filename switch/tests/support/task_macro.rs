// Hand-written stand-in for `#[switch::task]`, expanding to the shape in
// TASKS_DESIGN.md §2. Accepts `async fn` and `fn -> impl Future<Output = ()>`.
// Shared by unit tests (via `extern crate self as switch`), integration tests,
// benches and examples.

#[allow(unused_macros)]
macro_rules! task {
    ($(#[$attr:meta])* $vis:vis async fn $name:ident($($arg:ident: $ty:ty),* $(,)?) $body:block) => {
        $(#[$attr])*
        $vis fn $name($($arg: $ty),*) -> ::switch::SpawnToken<impl ::switch::__private::Task> {
            async fn __task($($arg: $ty),*) $body
            task!(@expand __task, $($arg),*)
        }
    };
    ($(#[$attr:meta])* $vis:vis fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> impl Future<Output = ()> $body:block) => {
        $(#[$attr])*
        $vis fn $name($($arg: $ty),*) -> ::switch::SpawnToken<impl ::switch::__private::Task> {
            fn __task($($arg: $ty),*) -> impl ::std::future::Future<Output = ()> + 'static $body
            task!(@expand __task, $($arg),*)
        }
    };
    (@expand $inner:ident, $($arg:ident),*) => {{
        ::std::thread_local! {
            static POOL: ::switch::__private::Pool = const { ::switch::__private::Pool::new() };
        }
        struct Key;
        static WAKER: ::std::task::RawWakerVTable =
            ::switch::__private::waker_vtable::<Key, _, _>($inner);
        unsafe impl ::switch::__private::PoolKey for Key {
            fn pool() -> &'static ::std::thread::LocalKey<::switch::__private::Pool> {
                &POOL
            }
            fn waker() -> &'static ::std::task::RawWakerVTable {
                &WAKER
            }
        }
        // SAFETY: Key's pool is private to this function, which always produces one future type.
        unsafe { ::switch::__private::token::<Key, _, _>(move || $inner($($arg),*)) }
    }};
}
