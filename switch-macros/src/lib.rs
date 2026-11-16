//! Task attributes re-exported by `switch` and `switch-runtime`.
//!
//! Application code uses `#[switch::task]`; this crate implements the attribute.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, quote, quote_spanned};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Error, FnArg, GenericArgument, ItemFn, Pat, PathArguments, ReturnType, Token, Type,
    TypeImplTrait, TypeParamBound, parse_macro_input,
};

#[doc = include_str!("task.txt")]
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task_attribute(attr, item, quote!(::switch_runtime), "switch_runtime::task")
}

#[doc = include_str!("task.txt")]
#[doc(hidden)]
#[proc_macro_attribute]
pub fn facade_task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task_attribute(attr, item, quote!(::switch), "switch::task")
}

fn task_attribute(
    attr: TokenStream,
    item: TokenStream,
    crate_path: TokenStream2,
    attribute: &str,
) -> TokenStream {
    if !attr.is_empty() {
        return Error::new_spanned(
            TokenStream2::from(attr),
            format!("`#[{attribute}]` takes no arguments"),
        )
        .into_compile_error()
        .into();
    }
    let function = parse_macro_input!(item as ItemFn);
    expand(function, &crate_path, attribute).unwrap_or_else(Error::into_compile_error).into()
}

/// The task function becomes a function, with the same generics, building a token
/// from a closure that makes the future: the body moves into the closure (into an
/// async block for an `async fn`), with every argument rebound inside it. `token`
/// keys the pool by a marker closure's type and the future type. The expansion
/// declares no name the signature or body can see.
fn expand(function: ItemFn, crate_path: &TokenStream2, attribute: &str) -> syn::Result<TokenStream2> {
    let ItemFn {
        attrs,
        vis,
        mut sig,
        block,
    } = function;
    if let Some(span) = sig
        .constness
        .map(|token| token.span)
        .or(sig.unsafety.map(|token| token.span))
        .or(sig.abi.as_ref().map(Spanned::span))
    {
        return Err(Error::new(
            span,
            format!("`#[{attribute}]` functions cannot be const, unsafe or extern"),
        ));
    }
    let mut bindings = Vec::new();
    for input in &mut sig.inputs {
        let FnArg::Typed(input) = input else {
            return Err(Error::new(
                input.span(),
                format!("`#[{attribute}]` functions cannot take `self`"),
            ));
        };
        let Pat::Ident(pattern) = input.pat.as_mut() else {
            return Err(Error::new(
                input.pat.span(),
                format!("`#[{attribute}]` arguments must be plain identifiers"),
            ));
        };
        if pattern.by_ref.is_some() || pattern.subpat.is_some() {
            return Err(Error::new(
                pattern.span(),
                format!("`#[{attribute}]` arguments must be plain identifiers"),
            ));
        }
        let cfgs = input
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"));
        let ident = &pattern.ident;
        // `mut` moves to the rebinding, where the body uses the argument.
        let mutability = pattern.mutability.take();
        bindings.push(quote!(#(#cfgs)* let #mutability #ident = #ident;));
    }

    let (future, declared, output) = if sig.asyncness.is_some() {
        let (output, body) = match &sig.output {
            ReturnType::Default => (quote!(()), quote!(#block)),
            // A `let` of the declared type gives the body's value a coercion
            // context, as a function's return type does (`&[1, 2]` to `&[u8]`). A
            // `return` returns from the async block, so it gets none. `impl Trait`
            // cannot annotate a `let`; that output is inferred. The `_` name and
            // the `allow`s keep a diverging body (`panic!()`) free of lints that a
            // plain `async fn` would not have.
            ReturnType::Type(_, ty) if !has_impl(ty.to_token_stream()) => {
                let value = quote::format_ident!("_output", span = Span::mixed_site());
                let body = quote_spanned! {Span::mixed_site()=>
                    #[allow(clippy::diverging_sub_expression)]
                    let #value: #ty = #block;
                    #[allow(unreachable_code)]
                    return #value;
                };
                (quote!(#ty), body)
            }
            ReturnType::Type(_, ty) => (quote!(#ty), quote!(#block)),
        };
        (
            quote_spanned!(sig.ident.span()=> async move { #(#bindings)* #body }),
            None,
            output,
        )
    } else {
        let Some((bounds, output)) = future_bounds(&sig.output) else {
            return Err(Error::new_spanned(
                &sig.output,
                format!("`#[{attribute}]` functions must be `async fn` or return `impl Future<Output = T>`"),
            ));
        };
        // The closure cannot name its return type, so `token` checks the future.
        // The declared bounds still resolve here, as the user wrote them.
        let declared = quote!(let _: ::std::marker::PhantomData<dyn #bounds>;);
        (
            quote!({ #(#bindings)* #block }),
            Some(declared),
            quote!(#output),
        )
    };

    let name = &sig.ident;
    let generics = &sig.generics;
    let where_clause = &sig.generics.where_clause;
    let inputs = &sig.inputs;
    let build = quote::format_ident!("build", span = Span::mixed_site());
    // Spanned at the function name, so a future that is not `'static` or does not
    // resolve to the declared output is reported there.
    let token = quote_spanned!(name.span()=> #crate_path::SpawnToken<impl #crate_path::__private::Task<Output = #output>>);
    let closure = quote_spanned!(name.span()=> let #build = move || #future;);
    // The marker is a closure: its type is unique to this function and carries every
    // generic parameter of it, `impl Trait` arguments included, so each instantiation
    // has its own pool even when its future type does not depend on a parameter.
    let spawn = quote_spanned! {name.span()=>
        #crate_path::__private::token(|| {}, #build)
    };
    Ok(quote! {
        #(#attrs)*
        #vis fn #name #generics (#inputs) -> #token #where_clause {
            #declared
            #closure
            #spawn
        }
    })
}

/// The bounds of `impl Future<Output = T>`, optionally `+ 'static`, and `T`.
fn future_bounds(output: &ReturnType) -> Option<(&Punctuated<TypeParamBound, Token![+]>, &Type)> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };
    let Type::ImplTrait(TypeImplTrait { bounds, .. }) = ty.as_ref() else {
        return None;
    };
    let mut future = None;
    for bound in bounds {
        match bound {
            TypeParamBound::Lifetime(lifetime) if lifetime.ident == "static" => {}
            TypeParamBound::Trait(bound) if future.is_none() => {
                future = Some(future_output(&bound.path)?);
            }
            _ => return None,
        }
    }
    Some((bounds, future?))
}

/// `T` of a `Future<Output = T>` path.
fn future_output(path: &syn::Path) -> Option<&Type> {
    let segment = path.segments.last()?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return None;
    };
    if segment.ident != "Future" || arguments.args.len() != 1 {
        return None;
    }
    match &arguments.args[0] {
        GenericArgument::AssocType(output)
            if output.ident == "Output" && output.generics.is_none() =>
        {
            Some(&output.ty)
        }
        _ => None,
    }
}

/// Whether a type mentions `impl Trait`.
fn has_impl(tokens: TokenStream2) -> bool {
    tokens.into_iter().any(|token| match token {
        proc_macro2::TokenTree::Ident(ident) => ident == "impl",
        proc_macro2::TokenTree::Group(group) => has_impl(group.stream()),
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use proc_macro2::{TokenStream, TokenTree};
    use syn::parse_quote;

    fn has_unsafe(tokens: TokenStream) -> bool {
        tokens.into_iter().any(|token| match token {
            TokenTree::Ident(ident) => ident == "unsafe",
            TokenTree::Group(group) => has_unsafe(group.stream()),
            _ => false,
        })
    }

    #[test]
    fn expansion_contains_no_unsafe() {
        for function in [
            parse_quote!(
                async fn run(a: u8, mut b: Vec<u8>) {
                    b.push(a);
                }
            ),
            parse_quote!(
                fn run(a: u8) -> impl Future<Output = ()> + 'static {
                    async move {
                        let _ = a;
                    }
                }
            ),
        ] {
            let expanded = super::expand(function, &quote::quote!(::switch_runtime), "switch_runtime::task").unwrap();
            assert!(!has_unsafe(expanded));
        }
    }
}
