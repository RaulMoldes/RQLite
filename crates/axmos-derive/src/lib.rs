use proc_macro::TokenStream;
use proc_macro_error::proc_macro_error;

mod core;

#[proc_macro_error]
#[proc_macro_derive(AxmosDataType, attributes(null, non_arith, non_copy, non_hash))]
pub fn data_type_derive(input: TokenStream) -> TokenStream {
    let input = proc_macro2::TokenStream::from(input);
    core::data_type_impl(input).into()
}
