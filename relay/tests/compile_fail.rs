/// A second `recv` while a `Message` is held does not compile.
#[test]
fn borrow_rules() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
