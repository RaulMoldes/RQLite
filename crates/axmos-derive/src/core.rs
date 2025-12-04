//! Procedural macro for deriving Axmos DataType infrastructure.
//!
//! Generates: DataTypeKind, DataTypeRef, DataTypeRefMut, reinterpret_cast,
//! reinterpret_cast_mut, and all helper methods using trait dispatch.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Attribute, Data, DeriveInput, Fields, Ident, Type, Variant, Visibility, parse2};

pub struct EnumInfo {
    name: Ident,
    vis: Visibility,
    variants: Vec<VariantInfo>,
}

pub struct VariantInfo {
    name: Ident,
    inner_type: Option<Type>,
    is_null: bool,
}

fn is_null(attrs: &[Attribute]) -> bool {
    let mut is_null = false;

    for attr in attrs {
        if attr.path().is_ident("null") {
            is_null = true;
        }
    }

    is_null
}

impl VariantInfo {
    fn from_variant(variant: Variant) -> syn::Result<Self> {
        let name = variant.ident;
        let inner_type = match variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                Some(fields.unnamed.into_iter().next().unwrap().ty)
            }
            Fields::Unit => None,
            _ => {
                return Err(syn::Error::new_spanned(
                    &name,
                    "Only unit variants or single-field tuple variants supported",
                ));
            }
        };

        let is_null = is_null(&variant.attrs);

        Ok(VariantInfo {
            name,
            inner_type,
            is_null,
        })
    }
}

impl EnumInfo {
    fn from_input(input: DeriveInput) -> syn::Result<Self> {
        let Data::Enum(data) = input.data else {
            return Err(syn::Error::new_spanned(
                input.ident,
                "AxmosDataType only works with enums",
            ));
        };

        let variants = data
            .variants
            .into_iter()
            .map(VariantInfo::from_variant)
            .collect::<syn::Result<Vec<_>>>()?;

        Ok(EnumInfo {
            name: input.ident,
            vis: input.vis,
            variants,
        })
    }

    fn kind_name(&self) -> Ident {
        format_ident!("{}Kind", &self.name)
    }

    fn ref_name(&self) -> Ident {
        format_ident!("{}Ref", &self.name)
    }

    fn ref_mut_name(&self) -> Ident {
        format_ident!("{}RefMut", &self.name)
    }

    fn gen_is_null(&self) -> TokenStream {
        // [is_null] using the [null] type attr
        let is_null_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.is_null {
                    quote! { Self::#vn => true }
                } else {
                    quote! { Self::#vn => false }
                }
            })
            .collect();

        quote! { #[inline]
        pub const fn is_null(self) -> bool {
            match self {
                #(#is_null_arms,)*
            }
        }}
    }

    fn gen_kind_enum(&self) -> TokenStream {
        let name = self.kind_name();
        let vis = &self.vis;
        let variant_names: Vec<_> = self.variants.iter().map(|v| &v.name).collect();
        let variant_values: Vec<u8> = (0..variant_names.len() as u8).collect();

        let variant_defs: Vec<_> = variant_names
            .iter()
            .zip(variant_values.iter())
            .map(|(vn, &val)| quote! { #vn = #val })
            .collect();

        // is_fixed_size
        let is_fixed_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    // Use trait dispatch: FIXED_SIZE.is_some() means fixed
                    quote! { Self::#vn => <#ty as AxmosValueType>::FIXED_SIZE.is_some() }
                } else {
                    // Unit variants (Null) are considered fixed with size 0
                    quote! { Self::#vn => true }
                }
            })
            .collect();

        // is_numeric
        let is_numeric_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    // Use trait dispatch: <Type as AxmosValueType>::IS_NUMERIC
                    quote! { Self::#vn => <#ty as AxmosValueType>::IS_NUMERIC }
                } else {
                    // Unit variants (Null) are not numeric
                    quote! { Self::#vn => false }
                }
            })
            .collect();

        // [size_of_val] using the FIXED_SIZE constant from AxmosValueType trait
        let size_of_val_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    // Use trait dispatch: <Type as AxmosValueType>::FIXED_SIZE
                    quote! { Self::#vn => <#ty as AxmosValueType>::FIXED_SIZE }
                } else {
                    // Unit variants (Null) have size 0
                    quote! { Self::#vn => Some(0) }
                }
            })
            .collect();

        // [is_null] using the [null] type attr
        let is_null_fn = self.gen_is_null();

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


                #is_null_fn


                pub fn from_repr(value: u8) -> ::std::io::Result<Self> {
                    match value {
                        #(#variant_values => Ok(Self::#variant_names),)*
                        _ => Err(::std::io::Error::new(
                            ::std::io::ErrorKind::InvalidData,
                            format!("Invalid {} value: {}", stringify!(#name), value),
                        )),
                    }
                }

                #[inline]
                pub const fn is_fixed_size(&self) -> bool {
                    match self {
                        #(#is_fixed_arms,)*
                    }
                }

                #[inline]
                pub const fn is_numeric(&self) -> bool {
                    match self {
                        #(#is_numeric_arms,)*
                    }
                }

                pub fn size_of_val(&self) -> Option<usize> {
                    match self {
                        #(#size_of_val_arms,)*
                    }
                }

                pub const fn variants() -> &'static [Self] {
                    &[#(Self::#variant_names,)*]
                }

                pub const fn name(self) -> &'static str {
                    match self {
                        #(Self::#variant_names => stringify!(#variant_names),)*
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
                type Error = ::std::io::Error;

                fn try_from(value: u8) -> Result<Self, Self::Error> {
                    Self::from_repr(value)
                }
            }

            impl ::std::fmt::Display for #name {
                fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                    write!(f, "{}", self.name())
                }
            }
        }
    }

    fn gen_ref_enum(&self) -> TokenStream {
        let name = self.ref_name();
        let vis = &self.vis;

        let variants: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    // Use trait associated type: <Type as AxmosValueType>::Ref<'a>
                    quote! { #vn(<#ty as AxmosValueType>::Ref<'a>) }
                } else {
                    quote! { #vn }
                }
            })
            .collect();

        quote! {
            #[derive(Debug, PartialEq)]
            #vis enum #name<'a> {
                #(#variants,)*
            }
        }
    }

    fn gen_ref_mut_enum(&self) -> TokenStream {
        let name = self.ref_mut_name();
        let vis = &self.vis;

        let variants: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    // Use trait associated type: <Type as AxmosValueType>::RefMut<'a>
                    quote! { #vn(<#ty as AxmosValueType>::RefMut<'a>) }
                } else {
                    quote! { #vn }
                }
            })
            .collect();

        quote! {
            #[derive(Debug, PartialEq)]
            #vis enum #name<'a> {
                #(#variants,)*
            }
        }
    }

    fn gen_reinterpret_cast(&self) -> TokenStream {
        let kind_name = self.kind_name();
        let ref_name = self.ref_name();

        let arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! {
                        #kind_name::#vn => {
                            let (value, size) = <#ty as AxmosValueType>::reinterpret(buffer)?;
                            Ok((#ref_name::#vn(value), size))
                        }
                    }
                } else {
                    // Unit variant (Null)
                    quote! {
                        #kind_name::#vn => Ok((#ref_name::#vn, 0))
                    }
                }
            })
            .collect();

        quote! {
            pub fn reinterpret_cast<'a>(
                dtype: #kind_name,
                buffer: &'a [u8],
            ) -> ::std::io::Result<(#ref_name<'a>, usize)> {
                match dtype {
                    #(#arms,)*
                }
            }
        }
    }

    fn gen_reinterpret_cast_mut(&self) -> TokenStream {
        let kind_name = self.kind_name();
        let ref_mut_name = self.ref_mut_name();

        let arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! {
                        #kind_name::#vn => {
                            let (value, size) = <#ty as AxmosValueType>::reinterpret_mut(buffer)?;
                            Ok((#ref_mut_name::#vn(value), size))
                        }
                    }
                } else {
                    // Unit variant (Null)
                    quote! {
                        #kind_name::#vn => Ok((#ref_mut_name::#vn, 0))
                    }
                }
            })
            .collect();

        quote! {
            pub fn reinterpret_cast_mut<'a>(
                dtype: #kind_name,
                buffer: &'a mut [u8],
            ) -> ::std::io::Result<(#ref_mut_name<'a>, usize)> {
                match dtype {
                    #(#arms,)*
                }
            }
        }
    }

    fn gen_impl_block(&self) -> TokenStream {
        let name = &self.name;
        let kind_name = self.kind_name();

        let kind_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(_) => #kind_name::#vn }
                } else {
                    quote! { Self::#vn => #kind_name::#vn }
                }
            })
            .collect();

        // For size(), we use AxmosValueType::value_size() via trait dispatch
        let size_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { Self::#vn(inner) => <#ty as AxmosValueType>::value_size(inner) }
                } else {
                    quote! { Self::#vn => 0 }
                }
            })
            .collect();

        // is_fixed_size: use FIXED_SIZE from AxmosValueType trait
        let is_fixed_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { Self::#vn(_) => <#ty as AxmosValueType>::FIXED_SIZE.is_some() }
                } else {
                    quote! { Self::#vn => true }
                }
            })
            .collect();

        let matches_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { (Self::#vn(_), #kind_name::#vn) => true }
                } else {
                    quote! { (Self::#vn, #kind_name::#vn) => true }
                }
            })
            .collect();

        quote! {
            impl #name {
                #[inline]
                pub fn kind(&self) -> #kind_name {
                    match self {
                        #(#kind_arms,)*
                    }
                }

                #[inline]
                pub fn size(&self) -> usize {
                    match self {
                        #(#size_arms,)*
                    }
                }



                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    match self {
                        #(#is_fixed_arms,)*
                    }
                }

                #[inline]
                pub fn is_null(&self) -> bool {
                    self.kind().is_null()
                }

                pub fn matches(&self, other: #kind_name) -> bool {
                    if self.is_null() {
                        return true
                    };

                    match (self, other) {
                        #(#matches_arms,)*
                        _ => false,
                    }
                }
            }
        }
    }

    fn gen_ref_impls(&self) -> TokenStream {
        let name = &self.name;
        let ref_name = self.ref_name();
        let ref_mut_name = self.ref_mut_name();
        let kind_name = self.kind_name();

        let kind_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(_) => #kind_name::#vn }
                } else {
                    quote! { Self::#vn => #kind_name::#vn }
                }
            })
            .collect();

        // to_owned: use AxmosValueTypeRef::to_owned() via trait dispatch
        let ref_to_owned_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => #name::#vn(AxmosValueTypeRef::to_owned(inner)) }
                } else {
                    quote! { Self::#vn => #name::#vn }
                }
            })
            .collect();

        // For size(), use AxmosValueTypeRef::size() via trait dispatch
        let ref_size_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => AxmosValueTypeRef::size(inner) }
                } else {
                    quote! { Self::#vn => 0 }
                }
            })
            .collect();

        // is_fixed_size for Ref
        let ref_is_fixed_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { Self::#vn(_) => <#ty as AxmosValueType>::FIXED_SIZE.is_some() }
                } else {
                    quote! { Self::#vn => true }
                }
            })
            .collect();

        let ref_matches_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { (Self::#vn(_), #kind_name::#vn) => true }
                } else {
                    quote! { (Self::#vn, #kind_name::#vn) => true }
                }
            })
            .collect();

        // to_owned for RefMut
        let ref_mut_to_owned_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => #name::#vn(AxmosValueTypeRefMut::to_owned(inner)) }
                } else {
                    quote! { Self::#vn => #name::#vn }
                }
            })
            .collect();

        let ref_mut_size_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => AxmosValueTypeRefMut::size(inner) }
                } else {
                    quote! { Self::#vn => 0 }
                }
            })
            .collect();

        quote! {
            impl<'a> #ref_name<'a> {
                #[inline]
                pub fn to_owned(&self) -> #name {
                    match self {
                        #(#ref_to_owned_arms,)*
                    }
                }


                 #[inline]
                pub fn kind(&self) -> #kind_name {
                    match self {
                        #(#kind_arms,)*
                    }
                }

                #[inline]
                pub fn size(&self) -> usize {
                    match self {
                        #(#ref_size_arms,)*
                    }
                }

                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    match self {
                        #(#ref_is_fixed_arms,)*
                    }
                }

                #[inline]
                pub fn is_null(&self) -> bool {
                    self.kind().is_null()
                }

                pub fn matches(&self, other: #kind_name) -> bool {
                    if self.is_null(){
                        return true;
                    };

                    match (self, other) {
                        #(#ref_matches_arms,)*
                        _ => false,
                    }
                }
            }

            impl<'a> #ref_mut_name<'a> {
                #[inline]
                pub fn to_owned(&self) -> #name {
                    match self {
                        #(#ref_mut_to_owned_arms,)*
                    }
                }

                #[inline]
                pub fn size(&self) -> usize {
                    match self {
                        #(#ref_mut_size_arms,)*
                    }
                }

                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    match self {
                        #(#ref_is_fixed_arms,)*
                    }
                }


                  #[inline]
                pub fn kind(&self) -> #kind_name {
                    match self {
                        #(#kind_arms,)*
                    }
                }

                #[inline]
                pub fn is_null(&self) -> bool {
                    self.kind().is_null()
                }

                pub fn matches(&self, other: #kind_name) -> bool {
                    if self.is_null(){
                        return true;
                    };

                    match (self, other) {
                        #(#ref_matches_arms,)*
                        _ => false,
                    }
                }
            }
        }
    }

    fn gen_as_ref_impls(&self) -> TokenStream {
        let name = &self.name;
        let ref_name = self.ref_name();
        let ref_mut_name = self.ref_mut_name();

        // For owned type, use AsRef<[u8]> which inner types implement
        let as_ref_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => inner.as_ref() }
                } else {
                    quote! { Self::#vn => &[] }
                }
            })
            .collect();

        let as_mut_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => inner.as_mut() }
                } else {
                    quote! { Self::#vn => &mut [] }
                }
            })
            .collect();

        // For Ref types, use AxmosValueTypeRef::as_bytes
        let ref_as_ref_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => AxmosValueTypeRef::as_bytes(inner) }
                } else {
                    quote! { Self::#vn => &[] }
                }
            })
            .collect();

        // For RefMut types, use AxmosValueTypeRefMut::as_bytes and as_bytes_mut
        let ref_mut_as_ref_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => AxmosValueTypeRefMut::as_bytes(inner) }
                } else {
                    quote! { Self::#vn => &[] }
                }
            })
            .collect();

        let ref_mut_as_mut_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => AxmosValueTypeRefMut::as_bytes_mut(inner) }
                } else {
                    quote! { Self::#vn => &mut [] }
                }
            })
            .collect();

        quote! {
            impl AsRef<[u8]> for #name {
                fn as_ref(&self) -> &[u8] {
                    match self {
                        #(#as_ref_arms,)*
                    }
                }
            }

            impl AsMut<[u8]> for #name {
                fn as_mut(&mut self) -> &mut [u8] {
                    match self {
                        #(#as_mut_arms,)*
                    }
                }
            }

            impl<'a> AsRef<[u8]> for #ref_name<'a> {
                fn as_ref(&self) -> &[u8] {
                    match self {
                        #(#ref_as_ref_arms,)*
                    }
                }
            }

            impl<'a> AsRef<[u8]> for #ref_mut_name<'a> {
                fn as_ref(&self) -> &[u8] {
                    match self {
                        #(#ref_mut_as_ref_arms,)*
                    }
                }
            }

            impl<'a> AsMut<[u8]> for #ref_mut_name<'a> {
                fn as_mut(&mut self) -> &mut [u8] {
                    match self {
                        #(#ref_mut_as_mut_arms,)*
                    }
                }
            }
        }
    }

    fn gen_coercion_impl(&self) -> TokenStream {
        let kind_name = self.kind_name();

        quote! {
            impl #kind_name {
                /// Check if this type can be coerced to the target type.
                pub fn can_be_coerced(&self, other: Self) -> bool {
                    if ::std::mem::discriminant(self) == ::std::mem::discriminant(&other) {
                        return true;
                    }

                    if self.is_fixed_size() || self.is_null() || other.is_null() {
                        return true;
                    }

                    false
                }
            }
        }
    }
}

pub fn data_type_impl(input: TokenStream) -> TokenStream {
    let input = match parse2::<DeriveInput>(input) {
        Ok(input) => input,
        Err(e) => return e.to_compile_error(),
    };

    let info = match EnumInfo::from_input(input) {
        Ok(info) => info,
        Err(e) => return e.to_compile_error(),
    };

    let kind_enum = info.gen_kind_enum();
    let ref_enum = info.gen_ref_enum();
    let ref_mut_enum = info.gen_ref_mut_enum();
    let reinterpret_cast = info.gen_reinterpret_cast();
    let reinterpret_cast_mut = info.gen_reinterpret_cast_mut();
    let impl_block = info.gen_impl_block();
    let ref_impls = info.gen_ref_impls();
    let as_ref_impls = info.gen_as_ref_impls();
    let coercion_impl = info.gen_coercion_impl();

    quote! {
        #kind_enum
        #ref_enum
        #ref_mut_enum
        #reinterpret_cast
        #reinterpret_cast_mut
        #impl_block
        #ref_impls
        #as_ref_impls
        #coercion_impl
    }
}
