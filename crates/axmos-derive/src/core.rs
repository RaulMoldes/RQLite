//! Procedural macro for deriving Axmos DataType infrastructure.
//!
//! Generates: DataTypeKind, DataTypeRef, DataTypeRefMut, reinterpret_cast,
//! reinterpret_cast_mut, and all helper methods using trait dispatch.

use std::collections::HashSet;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Error as SynError, Fields, Ident, Result as SynResult, Type,
    Variant, Visibility, parse2,
};

// Generate checked operations
const ARITHMETIC_OPS: [(&str, &str, &str); 5] = [
    ("checked_add", "Add", "add"),
    ("checked_sub", "Sub", "sub"),
    ("checked_mul", "Mul", "mul"),
    ("checked_div", "Div", "div"),
    ("checked_rem", "Rem", "rem"),
];

pub struct EnumInfo {
    name: Ident,
    vis: Visibility,
    variants: Vec<VariantInfo>,
}

pub struct VariantInfo {
    name: Ident,
    inner_type: Option<Type>,
    attrs: HashSet<String>,
}

impl VariantInfo {
    fn new(name: Ident, inner_type: Option<Type>) -> Self {
        Self {
            name,
            inner_type,
            attrs: HashSet::new(),
        }
    }

    // Populate the attribute set of this enum variant
    fn with_attrs(mut self, attrs: &[Attribute]) -> Self {
        for attr in attrs {
            if attr.path().is_ident("null") {
                self.attrs.insert("null".to_string());
            }

            if attr.path().is_ident("non_arith") {
                self.attrs.insert("non_arith".to_string());
            }

            if attr.path().is_ident("non_hash") {
                self.attrs.insert("non_hash".to_string());
            }

            if attr.path().is_ident("non_copy") {
                self.attrs.insert("non_copy".to_string());
            }
        }
        self
    }

    fn from_variant(variant: Variant) -> SynResult<Self> {
        let name = variant.ident;
        let inner_type = match variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                Some(fields.unnamed.into_iter().next().unwrap().ty)
            }
            Fields::Unit => None,
            _ => {
                return Err(SynError::new_spanned(
                    &name,
                    "Only unit variants or single-field tuple variants supported",
                ));
            }
        };
        Ok(VariantInfo::new(name, inner_type).with_attrs(&variant.attrs))
    }

    fn is_null(&self) -> bool {
        self.attrs.contains("is_null")
    }

    fn is_arith(&self) -> bool {
        !self.attrs.contains("non_arith")
    }

    fn is_copy(&self) -> bool {
        !self.attrs.contains("non_copy")
    }

    fn is_hashable(&self) -> bool {
        !self.attrs.contains("non_hash")
    }
}

impl EnumInfo {
    fn from_input(input: DeriveInput) -> SynResult<Self> {
        let Data::Enum(data) = input.data else {
            return Err(SynError::new_spanned(
                input.ident,
                "AxmosDataType only works with enums",
            ));
        };

        let variants = data
            .variants
            .into_iter()
            .map(VariantInfo::from_variant)
            .collect::<SynResult<Vec<_>>>()?;

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

    fn error_name(&self) -> Ident {
        format_ident!("{}Error", &self.name)
    }

    // Generates the [is_null] function
    fn gen_is_null(&self) -> TokenStream {
        // [is_null] using the [null] type attr
        let is_null_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.is_null() {
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

    fn gen_type_err(&self) -> TokenStream {
        let error_name = self.error_name();
        let kind_name = self.kind_name();
        let vis = &self.vis;

        quote! {
            /// Error type for DataType operations
            #[derive(Debug, Clone, PartialEq, Eq)]
            #vis enum #error_name {
                /// Attempted operation on Null variant
                NullOperation,
                /// Type mismatch between operands
                TypeMismatch {
                    left: #kind_name,
                    right: #kind_name,
                },
                /// Cannot cast between types
                CastError {
                    from: #kind_name,
                    to: #kind_name,
                },
                /// Type is not comparable
                NotComparable(#kind_name),
                /// Type does not support this operation
                UnsupportedOperation {
                    kind: #kind_name,
                    operation: &'static str,
                },
            }

            impl std::fmt::Display for #error_name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    match self {
                        Self::NullOperation => write!(f, "cannot perform operation on Null"),
                        Self::TypeMismatch { left, right } => {
                            write!(f, "type mismatch: {} vs {}", left, right)
                        }
                        Self::CastError { from, to } => {
                            write!(f, "cannot cast {} to {}", from, to)
                        }
                        Self::NotComparable(k) => {
                            write!(f, "type {} is not comparable", k)
                        }
                        Self::UnsupportedOperation { kind, operation } => {
                            write!(f, "type {} does not support {}", kind, operation)
                        }
                    }
                }
            }

            impl std::error::Error for #error_name {}
        }
    }

    fn gen_is_numeric_arms(&self) -> Vec<TokenStream> {
        // is_numeric
        let is_numeric_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    // <Type as AxmosValueType>::IS_NUMERIC
                    quote! { Self::#vn => <#ty as AxmosValueType>::IS_NUMERIC }
                } else {
                    // Unit variants (Null) are not numeric
                    quote! { Self::#vn => false }
                }
            })
            .collect();

        is_numeric_arms
    }

    fn gen_is_signed_arms(&self) -> Vec<TokenStream> {
        let is_signed_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { Self::#vn => <#ty as AxmosValueType>::IS_SIGNED }
                } else {
                    quote! { Self::#vn => false }
                }
            })
            .collect();

        is_signed_arms
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
                    // FIXED_SIZE.is_some() means fixed
                    quote! { Self::#vn => <#ty as AxmosValueType>::FIXED_SIZE.is_some() }
                } else {
                    // Unit variants (Null) are considered fixed with size 0
                    quote! { Self::#vn => true }
                }
            })
            .collect();

        // is_numeric
        let is_numeric_arms: Vec<_> = self.gen_is_numeric_arms();
        // is_signed
        let is_signed_arms: Vec<_> = self.gen_is_signed_arms();

        // [size_of_val] using the FIXED_SIZE constant from AxmosValueType trait
        let size_of_val_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    //  <Type as AxmosValueType>::FIXED_SIZE
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


                pub fn from_repr(value: u8) -> std::io::Result<Self> {
                    match value {
                        #(#variant_values => Ok(Self::#variant_names),)*
                        _ => Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
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


                 #[inline]
                pub const fn is_signed(&self) -> bool {
                    match self {
                        #(#is_signed_arms,)*
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
            ) -> std::io::Result<(#ref_name<'a>, usize)> {
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
            ) -> std::io::Result<(#ref_mut_name<'a>, usize)> {
                match dtype {
                    #(#arms,)*
                }
            }
        }
    }

    fn gen_try_cast(&self) -> TokenStream {
        let name = &self.name;
        let kind_name = self.kind_name();
        let error_name = self.error_name();

        // Generate match arms for casting
        // Each variant tries to cast via the AxmosCastable trait
        let cast_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    // For each target variant, try casting
                    let target_arms: Vec<_> = self
                        .variants
                        .iter()
                        .filter(|tv| tv.inner_type.is_some())
                        .map(|tv| {
                            let tvn = &tv.name;
                            let tty = tv.inner_type.as_ref().unwrap();
                            quote! {
                                #kind_name::#tvn => {
                                    <#ty as AxmosCastable<#tty>>::try_cast(inner)
                                        .map(#name::#tvn)
                                        .ok_or_else(|| #error_name::CastError {
                                            from: #kind_name::#vn,
                                            to: target,
                                        })
                                }
                            }
                        })
                        .collect();

                    quote! {
                        Self::#vn(inner) => {
                            match target {
                                #(#target_arms)*
                                _ => Err(#error_name::CastError {
                                    from: self.kind(),
                                    to: target,
                                }),
                            }
                        }
                    }
                } else {
                    // Null variant
                    quote! {
                        Self::#vn => {
                            if target.is_null() {
                                Ok(Self::#vn)
                            } else {
                                Err(#error_name::CastError {
                                    from: #kind_name::#vn,
                                    to: target,
                                })
                            }
                        }
                    }
                }
            })
            .collect();

        quote! {
            impl #name {
                /// Try to cast this value to a different type.
                ///
                /// Uses the `AxmosCastable` trait for type conversion.
                /// Returns an error if the cast is not possible.
                pub fn try_cast(&self, target: #kind_name) -> Result<Self, #error_name> {
                    // Same type, no cast needed
                    if self.kind() == target {
                        return Ok(self.clone());
                    }

                    match self {
                        #(#cast_arms)*
                    }
                }

                /// Check if this value can be cast to the target type.
                pub fn can_cast_to(&self, target: #kind_name) -> bool {
                    self.kind() == target || self.kind().can_be_coerced(target)
                }
            }
        }
    }

    fn gen_to_f64_method(&self) -> TokenStream {
        let name = &self.name;

        let arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;

                if v.inner_type.is_some() && v.is_copy() {
                    quote! {
                        Self::#vn(v) => {

                            let value: f64 = (*v).into();
                            value
                        }
                    }
                } else if v.inner_type.is_some() {

                    quote! {
                        Self::#vn(_) => f64::NAN
                    }
                } else {
                    quote! {
                        Self::#vn => f64::NAN
                    }
                }
            })
            .collect();

        quote! {
            impl #name {

                pub fn to_f64(&self) -> f64 {
                    match self {
                        #(#arms,)*
                    }
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
                pub fn is_numeric(&self) -> bool {
                    self.kind().is_numeric()
                }

                #[inline]
                pub fn is_signed(&self) -> bool {
                    self.kind().is_signed()
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

        // PartialOrd for Ref types
        let ref_cmp_arms: Vec<_> = self
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! {
                        (Self::#vn(a), Self::#vn(b)) => a.partial_cmp(b)
                    }
                } else {
                    quote! {
                        (Self::#vn, Self::#vn) => Some(std::cmp::Ordering::Equal)
                    }
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


             impl<'a> ::std::cmp::PartialOrd for #ref_name<'a> {
                fn partial_cmp(&self, other: &Self) -> Option<::std::cmp::Ordering> {
                    if ::std::mem::discriminant(self) != ::std::mem::discriminant(other) {
                        return Some(self.kind().cmp(&other.kind()));
                    }

                    match (self, other) {
                        #(#ref_cmp_arms,)*
                        _ => None,
                    }
                }
            }


             impl<'a> std::cmp::PartialOrd for #ref_mut_name<'a> {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    if std::mem::discriminant(self) != std::mem::discriminant(other) {
                        return Some(self.kind().cmp(&other.kind()));
                    }

                    match (self, other) {
                        #(#ref_cmp_arms,)*
                        _ => None,
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
                    if std::mem::discriminant(self) == std::mem::discriminant(&other) {
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

    fn gen_arithmetic_ops(&self) -> TokenStream {
        let name = &self.name;
        let error_name = self.error_name();

        let checked_methods: Vec<_> = ARITHMETIC_OPS
            .iter()
            .map(|(method_name, trait_name, op_name)| {
                let method_ident = format_ident!("{}", method_name);
                let op_ident = format_ident!("{}", trait_name);

                let match_arms: Vec<_> = self
                    .variants
                    .iter()
                    .map(|v| {
                        let vn = &v.name;
                        if v.inner_type.is_some() && v.is_arith() {
                            quote! {
                                (Self::#vn(a), Self::#vn(b)) => {
                                    Ok(Self::#vn(std::ops::#op_ident::$op_ident(*a, *b)))
                                }
                            }
                            .to_string()
                            .replace("$op_ident", op_name)
                            .parse::<TokenStream>()
                            .unwrap()
                        } else if v.inner_type.is_some() {
                            quote! {
                                (Self::#vn(_), Self::#vn(_)) => Err(#error_name::NullOperation)
                            }
                        } else {
                            quote! {
                                (Self::#vn, Self::#vn) => Err(#error_name::NullOperation)
                            }
                        }
                    })
                    .collect();

                quote! {
                    /// Perform checked arithmetic, returning error on type mismatch.
                    pub fn #method_ident(&self, rhs: &Self) -> Result<Self, #error_name> {
                        match (self, rhs) {
                            #(#match_arms,)*
                            (left, right) => Err(#error_name::TypeMismatch {
                                left: left.kind(),
                                right: right.kind(),
                            }),
                        }
                    }
                }
            })
            .collect();

        // Generate std::ops trait implementations using the checked methods
        let trait_impls = quote! {
            impl std::ops::Add for #name {
                type Output = Self;
                fn add(self, rhs: Self) -> Self::Output {
                    self.checked_add(&rhs).expect("arithmetic add failed")
                }
            }

            impl std::ops::Add<&#name> for #name {
                type Output = Self;
                fn add(self, rhs: &#name) -> Self::Output {
                    self.checked_add(rhs).expect("arithmetic add failed")
                }
            }


            impl<'a> ::std::ops::Add<#name> for &'a #name {
                type Output = #name;
                fn add(self, rhs: #name) -> Self::Output {
                    self.checked_add(&rhs).expect("arithmetic add failed")
                }
            }

            impl<'a, 'b> ::std::ops::Add<&'b #name> for &'a #name {
                type Output = #name;
                fn add(self, rhs: &'b #name) -> Self::Output {
                    self.checked_add(rhs).expect("arithmetic add failed")
                }
            }

            impl std::ops::Sub for #name {
                type Output = Self;
                fn sub(self, rhs: Self) -> Self::Output {
                    self.checked_sub(&rhs).expect("arithmetic sub failed")
                }
            }

            impl std::ops::Sub<&#name> for #name {
                type Output = Self;
                fn sub(self, rhs: &#name) -> Self::Output {
                    self.checked_sub(rhs).expect("arithmetic sub failed")
                }
            }


             impl<'a> ::std::ops::Sub<#name> for &'a #name {
                type Output = #name;
                fn sub(self, rhs: #name) -> Self::Output {
                    self.checked_sub(&rhs).expect("arithmetic sub failed")
                }
            }

            impl<'a, 'b> ::std::ops::Sub<&'b #name> for &'a #name {
                type Output = #name;
                fn sub(self, rhs: &'b #name) -> Self::Output {
                    self.checked_sub(rhs).expect("arithmetic sub failed")
                }
            }

            impl std::ops::Mul for #name {
                type Output = Self;
                fn mul(self, rhs: Self) -> Self::Output {
                    self.checked_mul(&rhs).expect("arithmetic mul failed")
                }
            }

            impl std::ops::Mul<&#name> for #name {
                type Output = Self;
                fn mul(self, rhs: &#name) -> Self::Output {
                    self.checked_mul(rhs).expect("arithmetic mul failed")
                }
            }



            impl<'a> ::std::ops::Mul<#name> for &'a #name {
                type Output = #name;
                fn mul(self, rhs: #name) -> Self::Output {
                    self.checked_mul(&rhs).expect("arithmetic mul failed")
                }
            }

            impl<'a, 'b> ::std::ops::Mul<&'b #name> for &'a #name {
                type Output = #name;
                fn mul(self, rhs: &'b #name) -> Self::Output {
                    self.checked_mul(rhs).expect("arithmetic mul failed")
                }
            }

            impl std::ops::Div for #name {
                type Output = Self;
                fn div(self, rhs: Self) -> Self::Output {
                    self.checked_div(&rhs).expect("arithmetic div failed")
                }
            }

            impl std::ops::Div<&#name> for #name {
                type Output = Self;
                fn div(self, rhs: &#name) -> Self::Output {
                    self.checked_div(rhs).expect("arithmetic div failed")
                }
            }

             impl<'a> ::std::ops::Div<#name> for &'a #name {
                type Output = #name;
                fn div(self, rhs: #name) -> Self::Output {
                    self.checked_div(&rhs).expect("arithmetic div failed")
                }
            }

            impl<'a, 'b> ::std::ops::Div<&'b #name> for &'a #name {
                type Output = #name;
                fn div(self, rhs: &'b #name) -> Self::Output {
                    self.checked_div(rhs).expect("arithmetic div failed")
                }
            }

            impl std::ops::Rem for #name {
                type Output = Self;
                fn rem(self, rhs: Self) -> Self::Output {
                    self.checked_rem(&rhs).expect("arithmetic rem failed")
                }
            }

            impl std::ops::Rem<&#name> for #name {
                type Output = Self;
                fn rem(self, rhs: &#name) -> Self::Output {
                    self.checked_rem(rhs).expect("arithmetic rem failed")
                }
            }


              impl<'a> ::std::ops::Rem<#name> for &'a #name {
                type Output = #name;
                fn rem(self, rhs: #name) -> Self::Output {
                    self.checked_rem(&rhs).expect("arithmetic rem failed")
                }
            }

            impl<'a, 'b> ::std::ops::Rem<&'b #name> for &'a #name {
                type Output = #name;
                fn rem(self, rhs: &'b #name) -> Self::Output {
                    self.checked_rem(rhs).expect("arithmetic rem failed")
                }
            }

            impl std::ops::AddAssign for #name {
                fn add_assign(&mut self, rhs: Self) {
                    *self = self.checked_add(&rhs).expect("arithmetic add_assign failed");
                }
            }

            impl std::ops::AddAssign<&#name> for #name {
                fn add_assign(&mut self, rhs: &#name) {
                    *self = self.checked_add(rhs).expect("arithmetic add_assign failed");
                }
            }

            impl std::ops::SubAssign for #name {
                fn sub_assign(&mut self, rhs: Self) {
                    *self = self.checked_sub(&rhs).expect("arithmetic sub_assign failed");
                }
            }

            impl std::ops::SubAssign<&#name> for #name {
                fn sub_assign(&mut self, rhs: &#name) {
                    *self = self.checked_sub(rhs).expect("arithmetic sub_assign failed");
                }
            }

            impl std::ops::MulAssign for #name {
                fn mul_assign(&mut self, rhs: Self) {
                    *self = self.checked_mul(&rhs).expect("arithmetic mul_assign failed");
                }
            }

            impl std::ops::MulAssign<&#name> for #name {
                fn mul_assign(&mut self, rhs: &#name) {
                    *self = self.checked_mul(rhs).expect("arithmetic mul_assign failed");
                }
            }

            impl std::ops::DivAssign for #name {
                fn div_assign(&mut self, rhs: Self) {
                    *self = self.checked_div(&rhs).expect("arithmetic div_assign failed");
                }
            }

            impl std::ops::DivAssign<&#name> for #name {
                fn div_assign(&mut self, rhs: &#name) {
                    *self = self.checked_div(rhs).expect("arithmetic div_assign failed");
                }
            }

            impl std::ops::RemAssign for #name {
                fn rem_assign(&mut self, rhs: Self) {
                    *self = self.checked_rem(&rhs).expect("arithmetic rem_assign failed");
                }
            }

            impl std::ops::RemAssign<&#name> for #name {
                fn rem_assign(&mut self, rhs: &#name) {
                    *self = self.checked_rem(rhs).expect("arithmetic rem_assign failed");
                }
            }
        };

        quote! {
            impl #name {
                #(#checked_methods)*
            }

            #trait_impls
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

    let error_type = info.gen_type_err();
    let kind_enum = info.gen_kind_enum();
    let ref_enum = info.gen_ref_enum();
    let ref_mut_enum = info.gen_ref_mut_enum();
    let reinterpret_cast = info.gen_reinterpret_cast();
    let reinterpret_cast_mut = info.gen_reinterpret_cast_mut();
    let impl_block = info.gen_impl_block();
    let try_cast = info.gen_try_cast();
    let arithmetic_ops = info.gen_arithmetic_ops();
    let as_f64 = info.gen_to_f64_method();
    let ref_impls = info.gen_ref_impls();
    let as_ref_impls = info.gen_as_ref_impls();
    let coercion_impl = info.gen_coercion_impl();

    quote! {
        #error_type
        #kind_enum
        #as_f64
        #ref_enum
        #ref_mut_enum
        #reinterpret_cast
        #reinterpret_cast_mut
        #impl_block
        #try_cast
        #arithmetic_ops

        #ref_impls
        #as_ref_impls
        #coercion_impl
    }
}
