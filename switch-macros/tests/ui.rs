//! `#[switch_runtime::task]` accepts the supported forms and rejects the rest with a
//! compile error.

#[test]
fn task_macro_ui() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/pass_*.rs");
    cases.compile_fail("tests/ui/fail_*.rs");
}
