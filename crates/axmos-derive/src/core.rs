use proc_macro_error::abort;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Attribute, Data, DeriveInput, Fields, Ident, Type, Variant, Visibility, parse2};

pub struct EnumInfo {
    name: Ident,
    vis: Visibility,
    _attrs: Vec<Attribute>,
    variants: Vec<VariantInfo>,
}

pub struct VariantInfo {
    name: Ident,
    inner_type: Option<Type>,
    is_fixed_size: bool,
    size_expr: TokenStream,
}

fn parse_type_attrs(attrs: &[Attribute]) -> syn::Result<(bool, TokenStream)> {
    for attr in attrs {
        if attr.path().is_ident("fixed") {
            return Ok((true, quote! { std::mem::size_of_val(inner) }));
        } else if attr.path().is_ident("dynamic") {
            return Ok((false, quote! { inner.len() }));
        }
    }
    Ok((false, quote! { 0 }))
}

impl EnumInfo {
    fn from_input(input: DeriveInput) -> syn::Result<Self> {
        let Data::Enum(data) = input.data else {
            abort!(input.ident, "[def_data_type!] only works with enums");
        };

        let variants = data
            .variants
            .into_iter()
            .map(VariantInfo::from_variant)
            .collect::<syn::Result<Vec<_>>>()?;

        Ok(EnumInfo {
            name: input.ident,
            vis: input.vis,
            _attrs: input.attrs,
            variants,
        })
    }

    fn ref_name(&self) -> Ident {
        format_ident!("{}Ref", &self.name)
    }

    fn ref_mut_name(&self) -> Ident {
        format_ident!("{}RefMut", &self.name)
    }

    fn gen_ref_enum<F>(&self, name: Ident, generator: F) -> TokenStream
    where
        F: Fn(&Type) -> TokenStream,
    {
        let vis = &self.vis;
        let variants = self.variants.iter().map(|v| {
            let variant_name = &v.name;
            if let Some(inner) = &v.inner_type {
                let ref_type = generator(inner);
                quote! { #variant_name(#ref_type) }
            } else {
                quote! { #variant_name }
            }
        });

        quote! {
            #[derive(Debug, PartialEq)]
            #vis enum #name<'a> {
                #(#variants,)*
            }
        }
    }

    fn gen_as_ref(&self) -> TokenStream {
        let name = &self.name;
        let ref_name = self.ref_name();
        let ref_mut_name = self.ref_mut_name();

        let arms = self.variants.iter().map(|v| {
            let variant_name = &v.name;

            if v.inner_type.is_some() {
                quote! { Self::#variant_name(p) => p.as_ref() }
            } else {
                quote! { Self::#variant_name => &[] }
            }
        });

        let arms_mut = self.variants.iter().map(|v| {
            let variant_name = &v.name;

            if v.inner_type.is_some() {
                quote! { Self::#variant_name(p) => p.as_mut() }
            } else {
                quote! { Self::#variant_name => &mut [] }
            }
        });

        let ref_arms = arms.clone();
        let ref_mut_arms = arms.clone();
        let ref_mut_arms_mut = arms_mut.clone();

        quote! {
            impl AsRef<[u8]> for #name {
                fn as_ref(&self) -> &[u8] {
                    match self {
                        #(#arms,)*
                    }
                }
            }


            impl AsMut<[u8]> for #name {
                fn as_mut(&mut self) -> &mut [u8] {
                    match self {
                        #(#arms_mut,)*
                    }
                }
            }

            impl<'a> AsRef<[u8]> for #ref_name<'a> {
                fn as_ref(&self) -> &[u8] {
                    match self {
                        #(#ref_arms,)*
                    }
                }
            }


            impl<'a> AsRef<[u8]> for #ref_mut_name<'a> {
                fn as_ref(&self) -> &[u8] {
                    match self {
                        #(#ref_mut_arms,)*
                    }
                }
            }


            impl<'a> AsMut<[u8]> for #ref_mut_name<'a> {
                fn as_mut(&mut self) -> &mut [u8] {
                    match self {
                        #(#ref_mut_arms_mut,)*
                    }
                }
            }
        }
    }

    fn gen_impl_block(&self) -> TokenStream {
        let name = &self.name;
        let is_fixed_size_impl = self.gen_is_fixed_size();
        let size_impl = self.gen_size();
        let datatype_impl = self.gen_datatype();
        let matches_impl = self.gen_matches();

        quote! {
            impl #name {
                #is_fixed_size_impl
                #size_impl

                #[inline]
                pub fn is_null(&self) -> bool {
                    matches!(self, Self::Null)
                }

                #datatype_impl
                #matches_impl
            }
        }
    }

    fn gen_is_fixed_size(&self) -> TokenStream {
        let fixed_patterns: Vec<_> = self
            .variants
            .iter()
            .filter(|v| v.is_fixed_size)
            .map(|v| {
                let name = &v.name;
                quote! { Self::#name(_) }
            })
            .collect();

        if fixed_patterns.is_empty() {
            quote! {
                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    false
                }
            }
        } else {
            quote! {
                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    matches!(
                        self,
                        #(#fixed_patterns)|*
                    )
                }
            }
        }
    }

    fn gen_size(&self) -> TokenStream {
        let arms = self.variants.iter().map(|v| {
            let name = &v.name;
            let size_expr = &v.size_expr;

            if v.inner_type.is_some() {
                quote! { Self::#name(inner) => #size_expr }
            } else {
                quote! { Self::#name => #size_expr }
            }
        });

        quote! {
            #[inline]
            pub fn size(&self) -> usize {
                use std::mem::size_of;
                match self {
                    #(#arms,)*
                }
            }
        }
    }

    fn gen_datatype(&self) -> TokenStream {
        let arms = self.variants.iter().map(|v| {
            let name = &v.name;
            if v.inner_type.is_some() {
                quote! { Self::#name(_) => DataTypeKind::#name }
            } else {
                quote! { Self::#name => DataTypeKind::#name }
            }
        });

        quote! {
            pub fn datatype(&self) -> DataTypeKind {
                match self {
                    #(#arms,)*
                }
            }
        }
    }

    fn gen_matches(&self) -> TokenStream {
        let arms = self.variants.iter().map(|v| {
            let name = &v.name;
            if v.inner_type.is_some() {
                quote! { (Self::#name(_), DataTypeKind::#name) => true }
            } else {
                quote! { (Self::#name, DataTypeKind::#name) => true }
            }
        });

        quote! {
            pub fn matches(&self, other: DataTypeKind) -> bool {
                match (self, other) {
                    #(#arms,)*
                    (Self::Null, _) => true,
                    _ => false,
                }
            }
        }
    }

    fn generate_ref_impls(&self) -> TokenStream {
        let ref_name = self.ref_name();
        let ref_mut_name = self.ref_mut_name();

        let ref_impl = self.generate_impl(&ref_name);
        let ref_mut_impl = self.generate_impl(&ref_mut_name);

        quote! {
            #ref_impl
            #ref_mut_impl
        }
    }

    fn generate_impl(&self, ref_name: &Ident) -> TokenStream {
        let name = &self.name;
        let is_fixed_size = self.gen_is_fixed_size();
        let size = self.gen_size();
        let matches = self.gen_matches();
        let datatype = self.gen_datatype();
        let to_owned = self.gen_to_owned(name);

        quote! {
            impl<'a> #ref_name<'a> {
                #is_fixed_size
                #size
                #datatype
                #matches

                #[inline]
                pub fn is_null(&self) -> bool {
                    matches!(self, Self::Null)
                }

                #to_owned
            }
        }
    }

    fn gen_to_owned(&self, to_type: &Ident) -> TokenStream {
        let arms = self.variants.iter().map(|v| {
            let name = &v.name;

            if v.inner_type.is_some() {
                let conversion = quote! { p.to_owned() };
                quote! { Self::#name(p) => #to_type::#name(#conversion) }
            } else {
                quote! { Self::#name => #to_type::#name }
            }
        });

        quote! {
            #[inline]
            pub fn to_owned(&self) -> #to_type {
                match self {
                    #(#arms,)*
                }
            }
        }
    }
}

impl VariantInfo {
    fn from_variant(variant: Variant) -> syn::Result<Self> {
        let name = variant.ident;
        let inner_type = match variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                Some(fields.unnamed.into_iter().next().unwrap().ty)
            }
            Fields::Unit => None,
            _ => abort!(
                name,
                "Only unit variants or variants with single unnamed field are supported"
            ),
        };

        let (is_fixed_size, size_expr) = parse_type_attrs(&variant.attrs)?;

        Ok(VariantInfo {
            name,
            inner_type,
            is_fixed_size,
            size_expr,
        })
    }
}

fn make_ref_type(ty: &Type) -> TokenStream {
    let ty_str = quote!(#ty).to_string();
    let ref_type = format_ident!("{}Ref", ty_str);
    quote! { #ref_type<'a> }
}

fn make_ref_mut_type(ty: &Type) -> TokenStream {
    let ty_str = quote!(#ty).to_string();
    let ref_mut_type = format_ident!("{}RefMut", ty_str);
    quote! { #ref_mut_type<'a> }
}




pub fn data_type_impl(input: TokenStream) -> TokenStream {
    let input = parse2::<DeriveInput>(input).unwrap();
    let info = EnumInfo::from_input(input).unwrap();

    let ref_name = info.ref_name();
    let ref_mut_name = info.ref_mut_name();
    let ref_enum = info.gen_ref_enum(ref_name, make_ref_type);
    let ref_mut_enum = info.gen_ref_enum(ref_mut_name, make_ref_mut_type);
    let impl_block = info.gen_impl_block();
    let ref_impls = info.generate_ref_impls();
    let as_ref_impl = info.gen_as_ref();

    quote! {
        #ref_enum
        #ref_mut_enum
        #impl_block
        #ref_impls
        #as_ref_impl
    }
}
