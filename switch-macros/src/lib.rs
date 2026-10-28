//! `#[switch::task]`: turns a task function into a function returning a
//! `switch::SpawnToken`. Use it through `switch::task`.

use proc_macro::TokenStream;
use quote::{format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::{Error, Expr, FnArg, ItemFn, Pat, TypeImplTrait, parse_macro_input};

/// Marks a task function: `async fn` or `fn -> impl Future<Output = ()>`, with
/// `'static` arguments bound to plain identifiers and no generics.
///
/// `#[task(capacity = N)]` sets the most occupied slots of this function per
/// runtime (default: one 64 KiB block's slots).
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut capacity = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("capacity") && capacity.is_none() {
            capacity = Some(meta.value()?.parse::<Expr>()?);
            Ok(())
        } else {
            Err(meta.error("expected `capacity = N`"))
        }
    });
    parse_macro_input!(attr with parser);
    let function = parse_macro_input!(item as ItemFn);
    expand(capacity, function)
        .unwrap_or_else(Error::into_compile_error)
        .into()
}

// One thread-local pool serves every call, and statics are not per instantiation,
// so a generic task would share one pool across future types of different layouts.
const GENERIC: &str = "switch tasks cannot be generic: write a concrete wrapper task";

fn expand(capacity: Option<Expr>, function: ItemFn) -> syn::Result<proc_macro2::TokenStream> {
    let ItemFn {
        attrs,
        vis,
        mut sig,
        block,
    } = function;
    if let Some(where_clause) = &sig.generics.where_clause {
        return Err(Error::new(where_clause.span(), GENERIC));
    }
    if !sig.generics.params.is_empty() {
        return Err(Error::new(sig.generics.span(), GENERIC));
    }
    let mut arguments = Vec::new();
    for input in &sig.inputs {
        let FnArg::Typed(input) = input else {
            return Err(Error::new(
                input.span(),
                "`#[switch::task]` functions cannot take `self`",
            ));
        };
        let Pat::Ident(pattern) = input.pat.as_ref() else {
            return Err(Error::new(
                input.pat.span(),
                "`#[switch::task]` arguments must be plain identifiers",
            ));
        };
        if pattern.by_ref.is_some() || pattern.subpat.is_some() {
            return Err(Error::new(
                pattern.span(),
                "`#[switch::task]` arguments must be plain identifiers",
            ));
        }
        let mut impl_trait = ImplTrait(None);
        impl_trait.visit_type(&input.ty);
        if let Some(span) = impl_trait.0 {
            return Err(Error::new(span, GENERIC));
        }
        arguments.push(pattern.ident.clone());
    }

    let name = sig.ident.clone();
    let inputs = sig.inputs.clone();
    sig.ident = format_ident!("__task");
    let capacity = capacity.map(|capacity| quote!(const CAPACITY: ::std::option::Option<u32> = ::std::option::Option::Some(#capacity);));
    // Spanned at the function name, so wrong output types and non-'static
    // arguments are reported there rather than at the attribute.
    let token = quote_spanned!(name.span()=> ::switch::SpawnToken<impl ::switch::__private::Task>);
    let spawn = quote_spanned! {name.span()=>
        // Built outside the unsafe block, so the task's own calls stay checked.
        let build = move || __task(#(#arguments),*);
        // SAFETY: Key's pool is private to this function. Its future is `__task`'s, a
        // nested non-generic fn that cannot use an enclosing impl's generics, so every
        // call produces one future type. `pool` only returns a const thread-local.
        unsafe { ::switch::__private::token::<Key, _, _>(build) }
    };
    Ok(quote! {
        #(#attrs)*
        #vis fn #name(#inputs) -> #token {
            #sig #block
            ::std::thread_local! {
                static POOL: ::switch::__private::Pool = const { ::switch::__private::Pool::new() };
            }
            struct Key;
            unsafe impl ::switch::__private::PoolKey for Key {
                fn pool() -> &'static ::std::thread::LocalKey<::switch::__private::Pool> {
                    &POOL
                }
                #capacity
            }
            #spawn
        }
    })
}

/// Finds an `impl Trait` (an anonymous generic) in an argument type.
struct ImplTrait(Option<proc_macro2::Span>);

impl<'ast> Visit<'ast> for ImplTrait {
    fn visit_type_impl_trait(&mut self, node: &'ast TypeImplTrait) {
        self.0.get_or_insert(node.impl_token.span);
    }
}
