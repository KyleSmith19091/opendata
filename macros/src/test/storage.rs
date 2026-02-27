use proc_macro2::TokenStream;
use quote::quote;
use syn::{Ident, ItemFn, parse::Parse, Token};

/// Parsed arguments for the storage test macro
struct TestMacroArgs {
    merge_operator: Option<syn::Expr>,
}

impl Parse for TestMacroArgs {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut merge_operator = None;

        // handle empty args
        if input.is_empty() {
            return Ok(TestMacroArgs { merge_operator });
        }

        // parse merge_operator = path syntax
        if input.peek(Ident) { // check that next token is an identifier 
            // parse as identifier
            let key: Ident = input.parse()?;

            match key.to_string().as_str() {
                // parse as merge_operator = expression (Expr) 
                "merge_operator" => {
                    input.parse::<Token![=]>()?;
                    merge_operator = Some(input.parse()?);
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        &key,
                        format!("unsupported argument '{}'. Supported arguments: merge_operator", key),
                    ));
                }
            }
        }

        // check for any remaining unparsed tokens
        if !input.is_empty() {
            let remaining: proc_macro2::TokenStream = input.parse()?;
            return Err(syn::Error::new_spanned(
                &remaining,
                "unexpected tokens. Expected end of arguments",
            ));
        }

        Ok(TestMacroArgs { merge_operator })
    }
}

pub fn test_impl(args: TokenStream, input: TokenStream) -> TokenStream {
    // parse arguments to the macro (see Parse impl for TestMacroArgs for implementation)
    let args_parsed = syn::parse2::<TestMacroArgs>(args).expect("failed to parse macro args");

    // parse string below macro as free standing function
    let item_fn = syn::parse2::<ItemFn>(input).expect("failed to parse function");

    // grab the name of the function from signature
    let fn_name = &item_fn.sig.ident;

    // construct inner function name
    let fn_name_inner = Ident::new(&format!("{}_inner", fn_name), item_fn.sig.ident.span());

    // get statements from function body
    let body = item_fn.block.stmts.clone();

    // generate storage creation based on whether merge_operator was provided
    let storage_creation = if let Some(merge_op) = args_parsed.merge_operator {
        quote! {
            let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(
                common::storage::in_memory::InMemoryStorage::with_merge_operator(
                    std::sync::Arc::new(#merge_op)
                )
            );
        }
    } else {
        quote! {
            let storage: std::sync::Arc<dyn Storage> = std::sync::Arc::new(
                common::storage::in_memory::InMemoryStorage::default()
            );
        }
    };

    quote!{
         #[::tokio::test]
        #[allow(unused_must_use)]
        async fn #fn_name() {
            #storage_creation
            #fn_name_inner(storage.clone()).await;
            let _ = storage.close().await;
        }

        async fn #fn_name_inner(storage: std::sync::Arc<dyn Storage>) {
            #(#body)*
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_function() {
        let input = quote! {
            async fn my_test() {
                assert_eq!(1, 1);
            }
        };

        let output = test_impl(TokenStream::new(), input);
        let output_str = output.to_string();

        assert!(output_str.contains("tokio"), "Expected 'tokio' in output");
        assert!(output_str.contains("my_test"), "Expected 'my_test' in output");
        assert!(output_str.contains("my_test_inner"), "Expected 'my_test_inner' in output");
    }

    #[test]
    fn test_with_merge_operator() {
        let args = quote! { merge_operator = MyMergeOp };
        let input = quote! {
            async fn my_test() {
                assert_eq!(1, 1);
            }
        };

        let output = test_impl(args, input);
        let output_str = output.to_string();

        assert!(output_str.contains("InMemoryStorage"), "Expected 'InMemoryStorage' when merge_operator is specified");
        assert!(output_str.contains("MyMergeOp"), "Expected merge operator type in output");
        assert!(output_str.contains("my_test_inner"), "Expected 'my_test_inner' in output");
    }

    #[test]
    fn test_unsupported_argument() {
        let args = quote! { invalid_arg = value };
        let result = syn::parse2::<TestMacroArgs>(args);
        
        assert!(result.is_err(), "Should error on unsupported argument");
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("unsupported argument"), "Error should mention unsupported argument");
    }

    #[test]
    fn test_trailing_tokens() {
        let args = quote! { merge_operator = MyOp extra };
        let result = syn::parse2::<TestMacroArgs>(args);
        
        assert!(result.is_err(), "Should error on trailing tokens");
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("unexpected tokens"), "Error should mention unexpected tokens");
    }
}
