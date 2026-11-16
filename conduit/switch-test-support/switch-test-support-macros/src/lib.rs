//! The `#[pinned_test]` attribute, re-exported by `switch-test-support`; use it from there.
//!
//! `#[pinned_test]` turns a function into a `#[test]` whose body runs through `switch_test_support::run` on the pinned worker thread, labelled `<module path>::<function name>`, with the bound `switch_test_support::PINNED_TEST_BOUND`. `#[pinned_test(bound_ms = N)]` sets the bound to N milliseconds; any other argument is a compile error. A `#[test]` already on the function is removed; other attributes, such as `#[ignore]`, are kept.
//!
//! See `docs/contributing/writing-tests.md`.
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, LitInt};

/// Runs the test body on the pinned worker, bounded by `PINNED_TEST_BOUND`.
/// `bound_ms = N` replaces the bound; it exists so the hung-test path can be tested
/// in under a second.
#[proc_macro_attribute]
pub fn pinned_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut bound_ms: Option<LitInt> = None;
    let attr_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("bound_ms") {
            bound_ms = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("expected `bound_ms = <milliseconds>`"))
        }
    });
    parse_macro_input!(attr with attr_parser);
    let function = parse_macro_input!(item as ItemFn);
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = function;
    let attrs = attrs
        .iter()
        .filter(|attr| !attr.path().is_ident("test"));

    let name = &sig.ident;
    let bound = match bound_ms {
        Some(ms) => quote!(::std::time::Duration::from_millis(#ms)),
        None => quote!(::switch_test_support::PINNED_TEST_BOUND),
    };

    quote! {
        #[test]
        #(#attrs)*
        #vis #sig {
            ::switch_test_support::run(
                concat!(module_path!(), "::", stringify!(#name)),
                #bound,
                move || #block,
            )
        }
    }
    .into()
}
