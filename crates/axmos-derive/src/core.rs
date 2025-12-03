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

    fn kind_name(&self) -> Ident {
        format_ident!("{}Kind", &self.name)
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

    fn gen_kind_enum(&self) -> TokenStream {
    let name = self.kind_name();
    let vis = &self.vis;
    let variant_names: Vec<_> = self.variants.iter().map(|v| &v.name).collect();
    let variant_values: Vec<u8> = (0..variant_names.len() as u8).collect();

    let variant_defs: Vec<_> = variant_names.iter().zip(variant_values.iter())
        .map(|(variant_name, &value)| {
            quote! { #variant_name = #value }
        })
        .collect();

    let is_fixed_size = self.gen_is_fixed_size(true);


    quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(u8)]
        #vis enum #name {
            #(#variant_defs,)*
        }

        impl #name {

            #[inline]
            pub const fn as_repr(self) -> u8 {
                self as u8
            }



            #[inline]
            pub const fn value(self) -> u8 {
                self as u8
            }


            pub fn from_repr(value: u8) -> std::io::Result<Self> {
                match value {
                    #(
                        #variant_values => Ok(Self::#variant_names),
                    )*
                    _ => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid {} value: {}", stringify!(#name), value),
                    )),
                }
            }


            #is_fixed_size



            pub const fn variants() -> &'static [Self] {
                &[
                    #(Self::#variant_names,)*
                ]
            }


            pub const fn name(self) -> &'static str {
                match self {
                    #(
                        Self::#variant_names => stringify!(#variant_names),
                    )*
                }
            }
        }

        impl From<#name> for u8 {
            #[inline]
            fn from(value: #name) -> Self {
                value as u8
            }
        }

        impl TryFrom<u8> for #name {
            type Error = std::io::Error;

            fn try_from(value: u8) -> Result<Self, Self::Error> {
                Self::from_repr(value)
            }
        }

        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.name())
            }
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

     fn gen_kind(&self) -> TokenStream {
        let kind_name = self.kind_name();

        let arms = self.variants.iter().map(|v| {
            let variant_name = &v.name;
            if v.inner_type.is_some() {
                quote! { Self::#variant_name(_) => #kind_name::#variant_name }
            } else {
                quote! { Self::#variant_name => #kind_name::#variant_name }
            }
        });

        quote! {
            #[inline]
            pub fn kind(&self) -> #kind_name {
                match self {
                    #(#arms,)*
                }
            }
        }
    }


    fn gen_impl_block(&self) -> TokenStream {
        let name = &self.name;
        let is_fixed_size_impl = self.gen_is_fixed_size(false);
        let size_impl = self.gen_size();


        let matches_impl = self.gen_matches();
        let kind_impl = self.gen_kind();

        quote! {
            impl #name {
                #is_fixed_size_impl
                #size_impl
                #kind_impl

                #[inline]
                pub fn is_null(&self) -> bool {
                    matches!(self, Self::Null)
                }


                #matches_impl
            }
        }
    }

    fn gen_is_fixed_size(&self, for_kind: bool) -> TokenStream {
        let fixed_patterns: Vec<_> = self
            .variants
            .iter()
            .filter(|v| v.is_fixed_size)
            .map(|v| {
                let name = &v.name;
                if !for_kind {
                    quote! { Self::#name(_) }
                } else {
                    quote! { Self::#name }
                }
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
        let is_fixed_size = self.gen_is_fixed_size(false);
        let size = self.gen_size();
        let matches = self.gen_matches();

        let to_owned = self.gen_to_owned(name);

        quote! {
            impl<'a> #ref_name<'a> {
                #is_fixed_size
                #size

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
    let kind_enum = info.gen_kind_enum();
    quote! {
        #ref_enum
        #ref_mut_enum
        #kind_enum
        #impl_block
        #ref_impls
        #as_ref_impl
    }
}
