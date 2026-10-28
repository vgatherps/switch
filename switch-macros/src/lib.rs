//! `#[switch::task]`: turns a task function into a function returning a
//! `switch::SpawnToken`. Use it through `switch::task`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{quote, quote_spanned};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Error, Expr, FnArg, GenericArgument, ItemFn, Pat, PathArguments, ReturnType, Token, Type,
    TypeImplTrait, TypeParamBound, parse_macro_input,
};

/// Marks a task function: `async fn` returning `()`, or `fn` returning
/// `impl Future<Output = ()>`, with arguments bound to plain identifiers. Each
/// future type of a task function has its own pool.
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

/// The task function becomes a function, with the same generics, building a token
/// from a closure that makes the future: the body moves into the closure (into an
/// async block for an `async fn`), with every argument rebound inside it. The pool
/// marker lives in a nested block of its own, so the signature and body cannot
/// name it; `token` keys the pool by marker and future type.
fn expand(capacity: Option<Expr>, function: ItemFn) -> syn::Result<TokenStream2> {
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
            "`#[switch::task]` functions cannot be const, unsafe or extern",
        ));
    }
    let mut bindings = Vec::new();
    for input in &mut sig.inputs {
        let FnArg::Typed(input) = input else {
            return Err(Error::new(
                input.span(),
                "`#[switch::task]` functions cannot take `self`",
            ));
        };
        let Pat::Ident(pattern) = input.pat.as_mut() else {
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
        let cfgs = input
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"));
        let ident = &pattern.ident;
        // `mut` moves to the rebinding, where the body uses the argument.
        let mutability = pattern.mutability.take();
        bindings.push(quote!(#(#cfgs)* let #mutability #ident = #ident;));
    }

    let (future, declared) = if sig.asyncness.is_some() {
        if !returns_unit(&sig.output) {
            return Err(Error::new_spanned(
                &sig.output,
                "async `#[switch::task]` functions must return `()`",
            ));
        }
        (
            quote_spanned!(sig.ident.span()=> async move { #(#bindings)* #block }),
            None,
        )
    } else {
        let Some(bounds) = unit_future_bounds(&sig.output) else {
            return Err(Error::new_spanned(
                &sig.output,
                "`#[switch::task]` functions must be `async fn` or return `impl Future<Output = ()>`",
            ));
        };
        // The closure cannot name its return type, so `token` checks the future.
        // The declared bounds still resolve here, as the user wrote them.
        let declared = quote!(let _: ::std::marker::PhantomData<dyn #bounds>;);
        (quote!({ #(#bindings)* #block }), Some(declared))
    };

    let name = &sig.ident;
    let generics = &sig.generics;
    let where_clause = &sig.generics.where_clause;
    let inputs = &sig.inputs;
    let build = quote::format_ident!("build", span = Span::mixed_site());
    let capacity_local = quote::format_ident!("capacity", span = Span::mixed_site());
    let capacity = match capacity {
        Some(capacity) => quote_spanned! {capacity.span()=>
            ::std::option::Option::Some(const { ::switch::__private::Capacity::new(#capacity) })
        },
        None => quote!(::std::option::Option::None),
    };
    // Spanned at the function name, so a future that is not `'static` or does not
    // resolve to `()` is reported there.
    let token = quote_spanned!(name.span()=> ::switch::SpawnToken<impl ::switch::__private::Task>);
    let closure = quote_spanned!(name.span()=> let #build = move || #future;);
    let spawn = quote_spanned! {name.span()=>
        {
            #[derive(Clone, Copy)]
            struct Marker;
            ::switch::__private::token(Marker, #capacity_local, #build)
        }
    };
    Ok(quote! {
        #(#attrs)*
        #vis fn #name #generics (#inputs) -> #token #where_clause {
            #declared
            #closure
            let #capacity_local = #capacity;
            #spawn
        }
    })
}

/// `()` or nothing.
fn returns_unit(output: &ReturnType) -> bool {
    match output {
        ReturnType::Default => true,
        ReturnType::Type(_, ty) => is_unit(ty),
    }
}

/// The bounds of `impl Future<Output = ()>`, optionally `+ 'static`.
fn unit_future_bounds(output: &ReturnType) -> Option<&Punctuated<TypeParamBound, Token![+]>> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };
    let Type::ImplTrait(TypeImplTrait { bounds, .. }) = ty.as_ref() else {
        return None;
    };
    let mut future = 0;
    for bound in bounds {
        match bound {
            TypeParamBound::Lifetime(lifetime) if lifetime.ident == "static" => {}
            TypeParamBound::Trait(bound) if is_unit_future(&bound.path) => future += 1,
            _ => return None,
        }
    }
    (future == 1).then_some(bounds)
}

fn is_unit_future(path: &syn::Path) -> bool {
    let Some(segment) = path.segments.last() else {
        return false;
    };
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return false;
    };
    segment.ident == "Future"
        && arguments.args.len() == 1
        && matches!(&arguments.args[0], GenericArgument::AssocType(output)
            if output.ident == "Output" && output.generics.is_none() && is_unit(&output.ty))
}

fn is_unit(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
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
            let expanded = super::expand(Some(parse_quote!(4)), function).unwrap();
            assert!(!has_unsafe(expanded));
        }
    }
}
