//! Intrusive link structures and traits.

/// Sentinel value for "no link".
pub const NIL: u32 = u32::MAX;

/// Intrusive links for both list and heap structures.
///
/// For doubly-linked list: uses a (prev), b (next)
/// For binary heap: uses a (parent), b (left), c (right)
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct IntrusiveLinks {
    pub a: u32,
    pub b: u32,
    pub c: u32,
}

impl IntrusiveLinks {
    pub const fn new() -> Self {
        Self { a: NIL, b: NIL, c: NIL }
    }
}

/// Entry that can be linked into intrusive structures.
pub trait Linked {
    fn links(&self) -> &IntrusiveLinks;
    fn links_mut(&mut self) -> &mut IntrusiveLinks;
}

/// Entry with a deadline for timer structures.
pub trait Timed: Linked {
    fn deadline(&self) -> u64;
}

/// List-specific accessors.
pub trait ListLinked: Linked {
    #[inline]
    fn prev(&self) -> u32 {
        self.links().a
    }

    #[inline]
    fn next(&self) -> u32 {
        self.links().b
    }

    #[inline]
    fn set_prev(&mut self, v: u32) {
        self.links_mut().a = v;
    }

    #[inline]
    fn set_next(&mut self, v: u32) {
        self.links_mut().b = v;
    }
}

impl<T: Linked> ListLinked for T {}
