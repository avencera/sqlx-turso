//! Checked query macros for sqlx-turso

use proc_macro::TokenStream;
use proc_macro2::{Group, TokenStream as TokenStream2, TokenTree};
use quote::quote;
use sqlx_macros_core::query::{QueryDriver, QueryMacroInput, expand_input};
use syn::{
    Expr, LitStr, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
};

struct QueryInput {
    source: Punctuated<LitStr, Token![+]>,
    args: Vec<Expr>,
}

struct QueryAsInput {
    record: Type,
    query: QueryInput,
}

impl Parse for QueryInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let source = Punctuated::<LitStr, Token![+]>::parse_separated_nonempty(input)?;
        let args = parse_args(input)?;

        Ok(Self { source, args })
    }
}

impl Parse for QueryAsInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let record = input.parse()?;
        input.parse::<Token![,]>()?;
        let query = input.parse()?;

        Ok(Self { record, query })
    }
}

fn parse_args(input: ParseStream<'_>) -> syn::Result<Vec<Expr>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    input.parse::<Token![,]>()?;
    let args = Punctuated::<Expr, Token![,]>::parse_terminated(input)?;

    Ok(args.into_iter().collect())
}

fn query_tokens(input: QueryInput) -> TokenStream2 {
    let QueryInput { source, args } = input;

    quote! {
        source = #source,
        args = [#(#args),*]
    }
}

fn query_as_tokens(input: QueryAsInput) -> TokenStream2 {
    let QueryAsInput { record, query } = input;
    let QueryInput { source, args } = query;

    quote! {
        source = #source,
        args = [#(#args),*],
        record = #record
    }
}

fn query_file_tokens(input: QueryInput) -> TokenStream2 {
    let QueryInput { source, args } = input;

    quote! {
        source_file = #source,
        args = [#(#args),*]
    }
}

fn query_file_as_tokens(input: QueryAsInput) -> TokenStream2 {
    let QueryAsInput { record, query } = input;
    let QueryInput { source, args } = query;

    quote! {
        source_file = #source,
        args = [#(#args),*],
        record = #record
    }
}

fn expand_query(input: TokenStream2) -> TokenStream {
    let input: QueryMacroInput = match syn::parse2(input) {
        Ok(input) => input,
        Err(error) => return error.to_compile_error().into(),
    };

    let driver = QueryDriver::new::<sqlx_turso_core::Turso>();

    match expand_input(input, [driver].iter()) {
        Ok(tokens) => rewrite_sqlx_paths(tokens).into(),
        Err(error) => {
            let message = error.to_string();
            quote!(compile_error!(#message);).into()
        }
    }
}

fn rewrite_sqlx_paths(tokens: TokenStream2) -> TokenStream2 {
    let mut output = TokenStream2::new();
    let mut iter = tokens.into_iter().peekable();

    while let Some(token) = iter.next() {
        match token {
            TokenTree::Punct(first)
                if first.as_char() == ':' && iter.peek().is_some_and(is_colon_punct) =>
            {
                let second = iter.next().expect("peeked token must exist");
                if iter.peek().is_some_and(is_sqlx_ident) {
                    let _ = iter.next();
                    output.extend(quote!(::sqlx_turso::sqlx));
                } else {
                    output.extend([TokenTree::Punct(first), second]);
                }
            }
            TokenTree::Group(group) => {
                let mut rewritten =
                    Group::new(group.delimiter(), rewrite_sqlx_paths(group.stream()));
                rewritten.set_span(group.span());
                output.extend([TokenTree::Group(rewritten)]);
            }
            token => output.extend([token]),
        }
    }

    output
}

fn is_colon_punct(token: &TokenTree) -> bool {
    matches!(token, TokenTree::Punct(punct) if punct.as_char() == ':')
}

fn is_sqlx_ident(token: &TokenTree) -> bool {
    matches!(token, TokenTree::Ident(ident) if ident == "sqlx")
}

/// Expands to a checked Turso query
#[proc_macro]
pub fn query(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QueryInput);
    expand_query(query_tokens(input))
}

/// Expands to a checked Turso query mapped to a type
#[proc_macro]
pub fn query_as(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QueryAsInput);
    expand_query(query_as_tokens(input))
}

/// Expands to a checked Turso scalar query
#[proc_macro]
pub fn query_scalar(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QueryInput);
    let mut tokens = query_tokens(input);
    tokens.extend(quote!(, scalar = _));
    expand_query(tokens)
}

/// Expands to a checked Turso query loaded from a file
#[proc_macro]
pub fn query_file(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QueryInput);
    expand_query(query_file_tokens(input))
}

/// Expands to a checked Turso file query mapped to a type
#[proc_macro]
pub fn query_file_as(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as QueryAsInput);
    expand_query(query_file_as_tokens(input))
}
