// Hand-written stand-in for `#[switch::task]`, expanding to the items the attribute
// will generate. Accepts `async fn` and `fn -> impl Future<Output = ()>`, with an
// optional leading `capacity = N;` (most occupied slots of the function per runtime).
// Shared by unit tests (via `extern crate self as switch`), integration tests,
// benches and examples.

#[allow(unused_macros)]
macro_rules! task {
    (@item [$cap:expr] $(#[$attr:meta])* $vis:vis async fn $name:ident($($arg:ident: $ty:ty),* $(,)?) $body:block) => {
        $(#[$attr])*
        $vis fn $name($($arg: $ty),*) -> ::switch::SpawnToken<impl ::switch::__private::Task> {
            async fn __task($($arg: $ty),*) $body
            task!(@expand [$cap] __task, $($arg),*)
        }
    };
    (@item [$cap:expr] $(#[$attr:meta])* $vis:vis fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> impl Future<Output = ()> $body:block) => {
        $(#[$attr])*
        $vis fn $name($($arg: $ty),*) -> ::switch::SpawnToken<impl ::switch::__private::Task> {
            fn __task($($arg: $ty),*) -> impl ::std::future::Future<Output = ()> + 'static $body
            task!(@expand [$cap] __task, $($arg),*)
        }
    };
    (@expand [$cap:expr] $inner:ident, $($arg:ident),*) => {{
        ::std::thread_local! {
            static POOL: ::switch::__private::Pool = const { ::switch::__private::Pool::new() };
        }
        struct Key;
        unsafe impl ::switch::__private::PoolKey for Key {
            fn pool() -> &'static ::std::thread::LocalKey<::switch::__private::Pool> {
                &POOL
            }
            const CAPACITY: Option<u32> = $cap;
        }
        // SAFETY: Key's pool is private to this function, which always produces one future type.
        unsafe { ::switch::__private::token::<Key, _, _>(move || $inner($($arg),*)) }
    }};
    (capacity = $cap:expr; $($item:tt)*) => {
        task!(@item [Some($cap)] $($item)*);
    };
    ($($item:tt)*) => {
        task!(@item [None] $($item)*);
    };
}
