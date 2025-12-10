//! Procedural macro for deriving Axmos DataType infrastructure.
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Error as SynError, Fields, Ident, Result as SynResult, Type,
    Variant, Visibility, parse2,
};

const ARITHMETIC_OPS: [(&str, &str, &str); 5] = [
    ("checked_add", "Add", "add"),
    ("checked_sub", "Sub", "sub"),
    ("checked_mul", "Mul", "mul"),
    ("checked_div", "Div", "div"),
    ("checked_rem", "Rem", "rem"),
];

#[derive(Debug, Default)]
pub struct VariantAttrs {
    pub is_null: bool,
    pub non_arith: bool,
    pub non_copy: bool,
}

impl VariantAttrs {
    fn from_attrs(attrs: &[Attribute]) -> Self {
        let mut result = Self::default();
        for attr in attrs {
            if attr.path().is_ident("null") {
                result.is_null = true;
            }
            if attr.path().is_ident("non_arith") {
                result.non_arith = true;
            }
            if attr.path().is_ident("non_copy") {
                result.non_copy = true;
            }
        }
        result
    }
}

pub struct VariantInfo {
    pub name: Ident,
    pub inner_type: Option<Type>,
    pub attrs: VariantAttrs,
}

impl VariantInfo {
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

        Ok(Self {
            attrs: VariantAttrs::from_attrs(&variant.attrs),
            name,
            inner_type,
        })
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.attrs.is_null
    }

    #[inline]
    pub fn is_arith(&self) -> bool {
        !self.attrs.non_arith
    }

    #[inline]
    pub fn is_copy(&self) -> bool {
        !self.attrs.non_copy
    }
}

pub struct EnumInfo {
    pub name: Ident,
    pub vis: Visibility,
    pub variants: Vec<VariantInfo>,
}

impl EnumInfo {
    pub fn from_input(input: DeriveInput) -> SynResult<Self> {
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

        Ok(Self {
            name: input.ident,
            vis: input.vis,
            variants,
        })
    }

    pub fn kind_name(&self) -> Ident {
        format_ident!("{}Kind", &self.name)
    }

    pub fn ref_name(&self) -> Ident {
        format_ident!("{}Ref", &self.name)
    }

    pub fn ref_mut_name(&self) -> Ident {
        format_ident!("{}RefMut", &self.name)
    }

    pub fn variant_names(&self) -> Vec<&Ident> {
        self.variants.iter().map(|v| &v.name).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TypeVariant {
    Owned,
    Ref,
    RefMut,
}

trait Generator {
    fn generate(&self, info: &EnumInfo) -> TokenStream;
}

struct KindEnumGenerator;

impl Generator for KindEnumGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let kind_name = info.kind_name();
        let vis = &info.vis;
        let variant_names = info.variant_names();
        let variant_values: Vec<u8> = (0..variant_names.len() as u8).collect();

        let variant_defs: Vec<_> = variant_names
            .iter()
            .zip(variant_values.iter())
            .map(|(vn, &val)| quote! { #vn = #val })
            .collect();

        let is_null_arms = self.gen_is_null_arms(info);
        let is_numeric_arms = self.gen_is_numeric_arms(info);
        let class_arms = self.gen_class_arms(info);
        let is_fixed_arms = self.gen_is_fixed_arms(info);
        let size_of_val_arms = self.gen_size_of_val_arms(info);

        quote! {
            #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            #[repr(u8)]
            #vis enum #kind_name {
                #(#variant_defs,)*
            }

            impl #kind_name {
                #[inline]
                pub const fn as_repr(self) -> u8 {
                    self as u8
                }

                #[inline]
                pub const fn value(self) -> u8 {
                    self as u8
                }

                #[inline]
                pub const fn is_null(self) -> bool {
                    match self {
                        #(#is_null_arms,)*
                    }
                }

                pub fn from_repr(value: u8) -> crate::database::errors::TypeResult<Self> {
                    match value {
                        #(#variant_values => Ok(Self::#variant_names),)*
                        _ => Err(crate::database::errors::TypeError::Other(format!("Cannot convert from value {value} to DataType"))),
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
                pub const fn cls(&self) -> crate::types::core::NumClass {
                    match self {
                        #(#class_arms,)*
                    }
                }

                #[inline]
                pub const fn is_signed(&self) -> bool {
                    matches!(self.cls(), crate::types::core::NumClass::Signed | crate::types::core::NumClass::Float)
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

            impl From<#kind_name> for u8 {
                #[inline]
                fn from(value: #kind_name) -> Self {
                    value as u8
                }
            }

            impl TryFrom<u8> for #kind_name {
                type Error = crate::database::errors::TypeError;

                fn try_from(value: u8) -> Result<Self, Self::Error> {
                    Self::from_repr(value)
                }
            }

            impl std::fmt::Display for #kind_name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.name())
                }
            }
        }
    }
}

impl KindEnumGenerator {
    fn gen_is_null_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                let is_null = v.is_null();
                quote! { Self::#vn => #is_null }
            })
            .collect()
    }

    fn gen_is_numeric_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { Self::#vn => <#ty as crate::types::core::AxmosValueType>::IS_NUMERIC }
                } else {
                    quote! { Self::#vn => false }
                }
            })
            .collect()
    }

    fn gen_class_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { Self::#vn => <#ty as crate::types::core::AxmosValueType>::NUM_CLASS }
                } else {
                    quote! { Self::#vn => crate::types::core::NumClass::Unknown }
                }
            })
            .collect()
    }

    fn gen_is_fixed_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { Self::#vn => <#ty as crate::types::core::AxmosValueType>::FIXED_SIZE.is_some() }
                } else {
                    quote! { Self::#vn => true }
                }
            })
            .collect()
    }

    fn gen_size_of_val_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { Self::#vn => <#ty as crate::types::core::AxmosValueType>::FIXED_SIZE }
                } else {
                    quote! { Self::#vn => Some(0) }
                }
            })
            .collect()
    }
}

struct RefEnumsGenerator;

impl Generator for RefEnumsGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let ref_enum = self.gen_ref_enum(info);
        let ref_mut_enum = self.gen_ref_mut_enum(info);

        quote! {
            #ref_enum
            #ref_mut_enum
        }
    }
}

impl RefEnumsGenerator {
    fn gen_ref_enum(&self, info: &EnumInfo) -> TokenStream {
        let ref_name = info.ref_name();
        let vis = &info.vis;

        let variants: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { #vn(<#ty as crate::types::core::AxmosValueType>::Ref<'a>) }
                } else {
                    quote! { #vn }
                }
            })
            .collect();

        quote! {
            #[derive(Debug)]
            #vis enum #ref_name<'a> {
                #(#variants,)*
            }
        }
    }

    fn gen_ref_mut_enum(&self, info: &EnumInfo) -> TokenStream {
        let ref_mut_name = info.ref_mut_name();
        let vis = &info.vis;

        let variants: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! { #vn(<#ty as crate::types::core::AxmosValueType>::RefMut<'a>) }
                } else {
                    quote! { #vn }
                }
            })
            .collect();

        quote! {
            #[derive(Debug)]
            #vis enum #ref_mut_name<'a> {
                #(#variants,)*
            }
        }
    }
}

struct CastGenerator;

impl Generator for CastGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let reinterpret = self.gen_reinterpret_cast(info);
        let reinterpret_mut = self.gen_reinterpret_cast_mut(info);
        let try_cast = self.gen_try_cast(info);

        quote! {
            #reinterpret
            #reinterpret_mut
            #try_cast
        }
    }
}

impl CastGenerator {
    fn gen_reinterpret_cast(&self, info: &EnumInfo) -> TokenStream {
        let kind_name = info.kind_name();
        let ref_name = info.ref_name();

        let arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! {
                        #kind_name::#vn => {
                            let (value, size) = <#ty as crate::types::core::AxmosValueType>::reinterpret(buffer)?;
                            Ok((#ref_name::#vn(value), size))
                        }
                    }
                } else {
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

    fn gen_reinterpret_cast_mut(&self, info: &EnumInfo) -> TokenStream {
        let kind_name = info.kind_name();
        let ref_mut_name = info.ref_mut_name();

        let arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    quote! {
                        #kind_name::#vn => {
                            let (value, size) = <#ty as crate::types::core::AxmosValueType>::reinterpret_mut(buffer)?;
                            Ok((#ref_mut_name::#vn(value), size))
                        }
                    }
                } else {
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

    fn gen_try_cast(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let kind_name = info.kind_name();

        let cast_arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    let target_arms: Vec<_> = info
                        .variants
                        .iter()
                        .filter(|tv| tv.inner_type.is_some())
                        .map(|tv| {
                            let tvn = &tv.name;
                            let tty = tv.inner_type.as_ref().unwrap();
                            quote! {
                                #kind_name::#tvn => {
                                    <#ty as crate::types::core::AxmosCastable<#tty>>::try_cast(inner)
                                        .map(#name::#tvn)
                                        .ok_or_else(|| crate::database::errors::TypeError::CastError {
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
                                _ => Err(crate::database::errors::TypeError::CastError {
                                    from: self.kind(),
                                    to: target,
                                }),
                            }
                        }
                    }
                } else {
                    quote! {
                        Self::#vn => {
                            if target.is_null() {
                                Ok(Self::#vn)
                            } else {
                                Err(crate::database::errors::TypeError::CastError {
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
                pub fn try_cast(&self, target: #kind_name) -> Result<Self, crate::database::errors::TypeError> {
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
}

struct ComparisonGenerator;

impl Generator for ComparisonGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let owned_impls = self.gen_owned_impls(info);
        let ref_impls = self.gen_ref_impls(info);
        let ref_mut_impls = self.gen_ref_mut_impls(info);

        quote! {
            #owned_impls
            #ref_impls
            #ref_mut_impls
        }
    }
}

impl ComparisonGenerator {
    fn gen_owned_impls(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;

        let eq_arms = self.gen_eq_arms(info, TypeVariant::Owned);
        let cmp_arms = self.gen_cmp_arms(info, TypeVariant::Owned);
        let hash_arms = self.gen_hash_arms(info);

        quote! {
            impl Eq for #name {}

            impl PartialEq for #name {
                fn eq(&self, other: &Self) -> bool {
                    match (self, other) {
                        #(#eq_arms,)*
                        // Numeric cross-type comparison
                        (a, b) if a.is_numeric() && b.is_numeric() => a.to_f64() == b.to_f64(),
                        _ => false,
                    }
                }
            }

            impl PartialOrd for #name {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    match (self, other) {
                        #(#cmp_arms,)*
                        // Numeric cross-type comparison
                        (a, b) if a.is_numeric() && b.is_numeric() => a.to_f64().partial_cmp(&b.to_f64()),
                        _ => None,
                    }
                }
            }

            impl std::hash::Hash for #name {
                fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                    std::mem::discriminant(self).hash(state);
                    match self {
                        #(#hash_arms,)*
                    }
                }
            }
        }
    }

    fn gen_ref_impls(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let ref_name = info.ref_name();

        let eq_arms = self.gen_eq_arms(info, TypeVariant::Ref);
        let cmp_arms = self.gen_cmp_arms(info, TypeVariant::Ref);

        quote! {
            impl<'a> PartialEq for #ref_name<'a> {
                fn eq(&self, other: &Self) -> bool {
                    match (self, other) {
                        #(#eq_arms,)*
                        (a, b) if a.kind().is_numeric() && b.kind().is_numeric() => {
                            a.to_owned().to_f64() == b.to_owned().to_f64()
                        }
                        _ => false,
                    }
                }
            }

            impl<'a> Eq for #ref_name<'a> {}

            impl<'a> PartialOrd for #ref_name<'a> {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    match (self, other) {
                        #(#cmp_arms,)*
                        (a, b) if a.kind().is_numeric() && b.kind().is_numeric() => {
                            a.to_owned().to_f64().partial_cmp(&b.to_owned().to_f64())
                        }
                        _ => None,
                    }
                }
            }

            impl<'a> PartialEq<#name> for #ref_name<'a> {
                fn eq(&self, other: &#name) -> bool {
                    self.to_owned() == *other
                }
            }

            impl<'a> PartialEq<#ref_name<'a>> for #name {
                fn eq(&self, other: &#ref_name<'a>) -> bool {
                    *self == other.to_owned()
                }
            }
        }
    }

    fn gen_ref_mut_impls(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let ref_name = info.ref_name();
        let ref_mut_name = info.ref_mut_name();

        let eq_arms = self.gen_eq_arms(info, TypeVariant::RefMut);
        let cmp_arms = self.gen_cmp_arms(info, TypeVariant::RefMut);

        quote! {
            impl<'a> PartialEq for #ref_mut_name<'a> {
                fn eq(&self, other: &Self) -> bool {
                    match (self, other) {
                        #(#eq_arms,)*
                        (a, b) if a.kind().is_numeric() && b.kind().is_numeric() => {
                            a.to_owned().to_f64() == b.to_owned().to_f64()
                        }
                        _ => false,
                    }
                }
            }

            impl<'a> Eq for #ref_mut_name<'a> {}

            impl<'a> PartialOrd for #ref_mut_name<'a> {
                fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                    match (self, other) {
                        #(#cmp_arms,)*
                        (a, b) if a.kind().is_numeric() && b.kind().is_numeric() => {
                            a.to_owned().to_f64().partial_cmp(&b.to_owned().to_f64())
                        }
                        _ => None,
                    }
                }
            }

            impl<'a> PartialEq<#name> for #ref_mut_name<'a> {
                fn eq(&self, other: &#name) -> bool {
                    self.to_owned() == *other
                }
            }

            impl<'a> PartialEq<#ref_mut_name<'a>> for #name {
                fn eq(&self, other: &#ref_mut_name<'a>) -> bool {
                    *self == other.to_owned()
                }
            }

            impl<'a> PartialEq<#ref_name<'a>> for #ref_mut_name<'a> {
                fn eq(&self, other: &#ref_name<'a>) -> bool {
                    self.to_owned() == other.to_owned()
                }
            }

            impl<'a> PartialEq<#ref_mut_name<'a>> for #ref_name<'a> {
                fn eq(&self, other: &#ref_mut_name<'a>) -> bool {
                    self.to_owned() == other.to_owned()
                }
            }
        }
    }

    fn gen_eq_arms(&self, info: &EnumInfo, variant: TypeVariant) -> Vec<TokenStream> {
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                match (&v.inner_type, variant) {
                    (Some(_), TypeVariant::Owned) => {
                        quote! { (Self::#vn(a), Self::#vn(b)) => a == b }
                    }
                    (Some(_), TypeVariant::Ref) => {
                        // inner is <ty as crate::types::core::AxmosValueType>::Ref<'a>, which implements crate::types::core::AxmosValueTypeRef
                        // Call to_owned() method directly on inner
                        quote! {
                            (Self::#vn(a), Self::#vn(b)) => {
                                crate::types::core::AxmosValueTypeRef::to_owned(a) == crate::types::core::AxmosValueTypeRef::to_owned(b)
                            }
                        }
                    }
                    (Some(_), TypeVariant::RefMut) => {
                        quote! {
                            (Self::#vn(a), Self::#vn(b)) => {
                                crate::types::core::AxmosValueTypeRefMut::to_owned(a) == crate::types::core::AxmosValueTypeRefMut::to_owned(b)
                            }
                        }
                    }
                    (None, _) => quote! { (Self::#vn, Self::#vn) => true },
                }
            })
            .collect()
    }

    fn gen_cmp_arms(&self, info: &EnumInfo, variant: TypeVariant) -> Vec<TokenStream> {
        let mut arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                match (&v.inner_type, variant) {
                    (Some(_), TypeVariant::Owned) => {
                        quote! { (Self::#vn(a), Self::#vn(b)) => a.partial_cmp(b) }
                    }
                    (Some(_), TypeVariant::Ref) => {
                        quote! {
                            (Self::#vn(a), Self::#vn(b)) => {
                                crate::types::core::AxmosValueTypeRef::to_owned(a).partial_cmp(&crate::types::core::AxmosValueTypeRef::to_owned(b))
                            }
                        }
                    }
                    (Some(_), TypeVariant::RefMut) => {
                        quote! {
                            (Self::#vn(a), Self::#vn(b)) => {
                                crate::types::core::AxmosValueTypeRefMut::to_owned(a).partial_cmp(&crate::types::core::AxmosValueTypeRefMut::to_owned(b))
                            }
                        }
                    }
                    (None, _) => quote! { (Self::#vn, Self::#vn) => Some(std::cmp::Ordering::Equal) },
                }
            })
            .collect();

        // Add Null comparison rules
        if let Some(nv) = info.variants.iter().find(|v| v.is_null()) {
            let nvn = &nv.name;
            arms.push(quote! { (Self::#nvn, _) => Some(std::cmp::Ordering::Less) });
            arms.push(quote! { (_, Self::#nvn) => Some(std::cmp::Ordering::Greater) });
        }

        arms
    }

    fn gen_hash_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                match &v.inner_type {
                    Some(ty) => {
                        quote! { Self::#vn(inner) => <#ty as crate::types::core::AxmosHashable>::hash128(inner).hash(state) }
                    }
                    None => quote! { Self::#vn => {} },
                }
            })
            .collect()
    }
}

struct ConversionGenerator;

impl Generator for ConversionGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let to_f64 = self.gen_to_f64(info);
        let as_ref_impls = self.gen_as_ref_impls(info);

        quote! {
            #to_f64
            #as_ref_impls
        }
    }
}

impl ConversionGenerator {
    fn gen_to_f64(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let ref_name = info.ref_name();
        let kind_name = info.kind_name();

        let owned_arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                match (&v.inner_type, v.is_copy()) {
                    (Some(_), true) => quote! { Self::#vn(v) => (*v).into() },
                    (Some(_), false) => quote! { Self::#vn(_) => f64::NAN },
                    (None, _) => quote! { Self::#vn => f64::NAN },
                }
            })
            .collect();

        let ref_arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                match (&v.inner_type, v.is_copy()) {
                    (Some(_), true) => {
                        // v implements crate::types::core::AxmosValueTypeRef, call to_owned() and convert to f64
                        quote! { Self::#vn(v) => crate::types::core::AxmosValueTypeRef::to_owned(v).into() }
                    }
                    (Some(_), false) => quote! { Self::#vn(_) => f64::NAN },
                    (None, _) => quote! { Self::#vn => f64::NAN },
                }
            })
            .collect();

        quote! {
            impl #name {
                /// Convert to f64 for numeric operations.
                /// Returns NAN for non-numeric types.
                pub fn to_f64(&self) -> f64 {
                    match self {
                        #(#owned_arms,)*
                    }
                }
            }

            impl<'a> #ref_name<'a> {
                /// Convert to f64 for numeric operations.
                /// Returns NAN for non-numeric types.
                pub fn to_f64(&self) -> f64 {
                    match self {
                        #(#ref_arms,)*
                    }
                }
            }

            impl #kind_name {
                /// Convert raw bytes to f64 based on this type kind.
                /// Useful for histograms and statistics.
                pub fn bytes_to_f64(&self, bytes: &[u8]) -> Option<f64> {
                    let (value, _) = reinterpret_cast(*self, bytes).ok()?;
                    let f = value.to_f64();
                    if f.is_nan() { None } else { Some(f) }
                }
            }
        }
    }

    fn gen_as_ref_impls(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let ref_name = info.ref_name();
        let ref_mut_name = info.ref_mut_name();

        let as_ref_arms: Vec<_> = info
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

        let as_mut_arms: Vec<_> = info
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

        let ref_as_ref_arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => crate::types::core::AxmosValueTypeRef::as_bytes(inner) }
                } else {
                    quote! { Self::#vn => &[] }
                }
            })
            .collect();

        let ref_mut_as_ref_arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => crate::types::core::AxmosValueTypeRefMut::as_bytes(inner) }
                } else {
                    quote! { Self::#vn => &[] }
                }
            })
            .collect();

        let ref_mut_as_mut_arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(inner) => crate::types::core::AxmosValueTypeRefMut::as_bytes_mut(inner) }
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
}

struct ImplBlockGenerator;

impl Generator for ImplBlockGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let owned_impl = self.gen_owned_impl(info);
        let ref_impl = self.gen_ref_impl(info);
        let ref_mut_impl = self.gen_ref_mut_impl(info);

        quote! {
            #owned_impl
            #ref_impl
            #ref_mut_impl
        }
    }
}

struct OpsGenerator;

impl OpsGenerator {
    pub fn generate_ceil_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants.iter().map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    quote! { Self::#vn(inner) => Self::#vn(<#ty as crate::types::core::AxmosOps>::ceil(inner)) }
                } else {
                    quote! { Self::#vn(inner) => DataType::Null }
                }
            } else {
                quote! { Self::#vn => DataType::Null }
            }
        }).collect()
    }
    pub fn generate_abs_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants.iter().map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    quote! { Self::#vn(inner) => Self::#vn(<#ty as crate::types::core::AxmosOps>::abs(inner)) }
                } else {
                    quote! { Self::#vn(inner) => DataType::Null }
                }
            } else {
                quote! { Self::#vn => DataType::Null }
            }
        }).collect()
    }


    pub fn generate_sqrt_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants.iter().map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    quote! { Self::#vn(inner) => Self::#vn(<#ty as crate::types::core::AxmosOps>::sqrt(inner)) }
                } else {
                    quote! { Self::#vn(inner) => DataType::Null }
                }
            } else {
                quote! { Self::#vn => DataType::Null }
            }
        }).collect()
    }


    pub fn generate_round_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants.iter().map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    quote! { Self::#vn(inner) => Self::#vn(<#ty as crate::types::core::AxmosOps>::round(inner)) }
                } else {
                    quote! { Self::#vn(inner) => DataType::Null }
                }
            } else {
                quote! { Self::#vn => DataType::Null }
            }
        }).collect()
    }







     pub fn generate_floor_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        info.variants.iter().map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    quote! { Self::#vn(inner) => Self::#vn(<#ty as crate::types::core::AxmosOps>::floor(inner)) }
                } else {
                    quote! { Self::#vn(inner) => DataType::Null }
                }
            } else {
                quote! { Self::#vn => DataType::Null }
            }
        }).collect()
    }
}
impl Generator for OpsGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;

        let abs_arms: Vec<_> = self.generate_abs_arms(info);
        let sqrt_arms: Vec<_> = self.generate_sqrt_arms(info);
        let ceil_arms: Vec<_> = self.generate_ceil_arms(info);
        let floor_arms: Vec<_> = self.generate_floor_arms(info);
        let round_arms: Vec<_> = self.generate_round_arms(info);

        quote! {
            impl #name {
                /// Extract as signed integer if possible.
                pub(crate) fn abs(&self) -> Self {
                    match self {
                        #(#abs_arms,)*

                    }
                }

                /// Extract as signed integer if possible.
                pub(crate) fn ceil(&self) -> Self {
                    match self {
                        #(#ceil_arms,)*

                    }
                }


                /// Extract as signed integer if possible.
                pub(crate) fn floor(&self) -> Self {
                    match self {
                        #(#floor_arms,)*

                    }
                }


                 /// Extract as signed integer if possible.
                pub(crate) fn round(&self) -> Self {
                    match self {
                        #(#round_arms,)*

                    }
                }


                /// Extract as signed integer if possible.
                pub(crate) fn sqrt(&self) -> Self {
                    match self {
                        #(#sqrt_arms,)*

                    }
                }
            }
        }
    }
}

struct NumericTypeGenerator;

impl Generator for NumericTypeGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let kind_name = info.kind_name();

        // Generate extract_i64 arms
        let extract_i64_arms: Vec<_> = info.variants.iter().filter_map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    Some(quote! { Self::#vn(inner) => <#ty as crate::types::core::NumericType>::as_i64(inner) })
                } else {
                    None
                }
            } else {
                None
            }
        }).collect();

        // Generate extract_u64 arms
        let extract_u64_arms: Vec<_> = info.variants.iter().filter_map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    Some(quote! { Self::#vn(inner) => <#ty as crate::types::core::NumericType>::as_u64(inner) })
                } else {
                    None
                }
            } else {
                None
            }
        }).collect();

        // Generate extract_f64 arms
        let extract_f64_arms: Vec<_> = info.variants.iter().filter_map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    Some(quote! { Self::#vn(inner) => Some(<#ty as crate::types::core::NumericType>::as_f64(inner)) })
                } else {
                    None
                }
            } else {
                None
            }
        }).collect();

        // Generate from_number arms for each variant
        let from_signed_arms: Vec<_> = info.variants.iter().filter_map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    Some(quote! { #kind_name::#vn => Self::#vn(<#ty as crate::types::core::NumericType>::from_i64(v)) })
                } else {
                    None
                }
            } else {
                None
            }
        }).collect();

        let from_unsigned_arms: Vec<_> = info.variants.iter().filter_map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    Some(quote! { #kind_name::#vn => Self::#vn(<#ty as crate::types::core::NumericType>::from_u64(v)) })
                } else {
                    None
                }
            } else {
                None
            }
        }).collect();

        let from_float_arms: Vec<_> = info.variants.iter().filter_map(|v| {
            let vn = &v.name;
            if let Some(ty) = &v.inner_type {
                if v.is_arith() && v.is_copy() {
                    Some(quote! { #kind_name::#vn => Self::#vn(<#ty as crate::types::core::NumericType>::from_f64(v)) })
                } else {
                    None
                }
            } else {
                None
            }
        }).collect();

        quote! {
            impl #name {
                /// Extract as signed integer if possible.
                pub fn as_i64_checked(&self) -> Option<i64> {
                    match self {
                        #(#extract_i64_arms,)*
                        _ => None,
                    }
                }

                /// Extract as unsigned integer if possible.
                pub fn as_u64_checked(&self) -> Option<u64> {
                    match self {
                        #(#extract_u64_arms,)*
                        _ => None,
                    }
                }

                /// Extract as float.
                pub fn as_f64_checked(&self) -> Option<f64> {
                    match self {
                        #(#extract_f64_arms,)*
                        _ => None,
                    }
                }

                /// Create a DataType from a signed integer with the given type hint.
                pub fn from_i64_hinted(v: i64, hint: #kind_name) -> Self {
                    match hint {
                        #(#from_signed_arms,)*
                        _ => Self::BigInt(Int64::from(v)),
                    }
                }

                /// Create a DataType from an unsigned integer with the given type hint.
                pub fn from_u64_hinted(v: u64, hint: #kind_name) -> Self {
                    match hint {
                        #(#from_unsigned_arms,)*
                        _ => Self::BigUInt(UInt64::from(v)),
                    }
                }

                /// Create a DataType from a float with the given type hint.
                pub fn from_f64_hinted(v: f64, hint: #kind_name) -> Self {
                    match hint {
                        #(#from_float_arms,)*
                        _ => Self::Double(Float64::from(v)),
                    }
                }

                /// Perform a binary arithmetic operation with automatic type promotion.
                pub fn arithmetic_op<F>(&self, other: &Self, hint: Option<#kind_name>, op: F) -> crate::database::errors::TypeResult<Self>
                where
                    F: FnOnce(crate::types::core::Number, crate::types::core::Number) -> crate::types::core::Number,
                {
                    let c1 = self.cls();
                    let c2 = other.cls();
                    let promoted = c1.promote(c2);

                    let (l, r) = match promoted {
                        crate::types::core::NumClass::Signed => {
                            let l = self.as_i64_checked().ok_or_else(||     crate::database::errors::TypeError::Other(
                                "Cannot convert to signed".to_string()
                            ))?;
                            let r = other.as_i64_checked().ok_or_else(||     crate::database::errors::TypeError::Other(
                                "Cannot convert to signed".to_string()
                            ))?;
                            (crate::types::core::Number::Signed(l), crate::types::core::Number::Signed(r))
                        }
                        crate::types::core::NumClass::Unsigned => {
                            let l = self.as_u64_checked().ok_or_else(||     crate::database::errors::TypeError::Other(
                                "Cannot convert to unsigned".to_string()
                            ))?;
                            let r = other.as_u64_checked().ok_or_else(||     crate::database::errors::TypeError::Other(
                                "Cannot convert to unsigned".to_string()
                            ))?;
                            (crate::types::core::Number::Unsigned(l), crate::types::core::Number::Unsigned(r))
                        }
                        crate::types::core::NumClass::Float => {
                            let l = self.as_f64_checked().ok_or_else(||     crate::database::errors::TypeError::Other(
                                "Cannot convert to float".to_string()
                            ))?;
                            let r = other.as_f64_checked().ok_or_else(||     crate::database::errors::TypeError::Other(
                                "Cannot convert to float".to_string()
                            ))?;
                            (crate::types::core::Number::Float(l), crate::types::core::Number::Float(r))
                        },
                        crate::types::core::NumClass::Unknown => return Err(crate::database::errors::TypeError::Other(
                                "Invalid datatype".to_string()
                            ))
                    };

                    let result = op(l, r);
                    let result_hint = hint.unwrap_or_else(|| match promoted {
                        crate::types::core::NumClass::Signed => #kind_name::BigInt,
                        crate::types::core::NumClass::Unsigned => #kind_name::BigUInt,
                        crate::types::core::NumClass::Float => #kind_name::Double,
                        crate::types::core::NumClass::Unknown => panic!("Invalid datatype")
                    });

                    Ok(match result {
                        crate::types::core::Number::Signed(v) => Self::from_i64_hinted(v, result_hint),
                        crate::types::core::Number::Unsigned(v) => Self::from_u64_hinted(v, result_hint),
                        crate::types::core::Number::Float(v) => Self::from_f64_hinted(v, result_hint),
                    })
                }
            }
        }
    }
}

impl ImplBlockGenerator {
    fn gen_kind_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        let kind_name = info.kind_name();
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { Self::#vn(_) => #kind_name::#vn }
                } else {
                    quote! { Self::#vn => #kind_name::#vn }
                }
            })
            .collect()
    }

    fn gen_size_arms(&self, info: &EnumInfo, variant: TypeVariant) -> Vec<TokenStream> {
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if let Some(ty) = &v.inner_type {
                    match variant {
                        TypeVariant::Owned => {
                            quote! { Self::#vn(inner) => <#ty as crate::types::core::AxmosValueType>::value_size(inner) }
                        }
                        TypeVariant::Ref => {
                            quote! { Self::#vn(inner) => crate::types::core::AxmosValueTypeRef::size(inner) }
                        }
                        TypeVariant::RefMut => {
                            quote! { Self::#vn(inner) => crate::types::core::AxmosValueTypeRefMut::size(inner) }
                        }
                    }
                } else {
                    quote! { Self::#vn => 0 }
                }
            })
            .collect()
    }

    fn gen_matches_arms(&self, info: &EnumInfo) -> Vec<TokenStream> {
        let kind_name = info.kind_name();
        info.variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() {
                    quote! { (Self::#vn(_), #kind_name::#vn) => true }
                } else {
                    quote! { (Self::#vn, #kind_name::#vn) => true }
                }
            })
            .collect()
    }

    fn gen_to_owned_arms(&self, info: &EnumInfo, variant: TypeVariant) -> Vec<TokenStream> {
        let name = &info.name;
        info.variants
            .iter()
            .filter_map(|v| {
                let vn = &v.name;
                match (&v.inner_type, variant) {
                    (Some(_), TypeVariant::Ref) => {
                        Some(quote! { Self::#vn(inner) => #name::#vn(crate::types::core::AxmosValueTypeRef::to_owned(inner)) })
                    }
                    (Some(_), TypeVariant::RefMut) => {
                        Some(quote! { Self::#vn(inner) => #name::#vn(crate::types::core::AxmosValueTypeRefMut::to_owned(inner)) })
                    }
                    (None, TypeVariant::Ref | TypeVariant::RefMut) => {
                        Some(quote! { Self::#vn => #name::#vn })
                    }
                    _ => None,
                }
            })
            .collect()
    }

    fn gen_owned_impl(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let kind_name = info.kind_name();

        let kind_arms = self.gen_kind_arms(info);
        let size_arms = self.gen_size_arms(info, TypeVariant::Owned);
        let matches_arms = self.gen_matches_arms(info);

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
                pub fn cls(&self) -> crate::types::core::NumClass {
                    self.kind().cls()
                }

                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    self.kind().is_fixed_size()
                }

                #[inline]
                pub fn is_null(&self) -> bool {
                    self.kind().is_null()
                }

                pub fn matches(&self, other: #kind_name) -> bool {
                    if self.is_null() {
                        return true;
                    }
                    match (self, other) {
                        #(#matches_arms,)*
                        _ => false,
                    }
                }
            }
        }
    }

    fn gen_ref_impl(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let ref_name = info.ref_name();
        let kind_name = info.kind_name();

        let kind_arms = self.gen_kind_arms(info);
        let size_arms = self.gen_size_arms(info, TypeVariant::Ref);
        let matches_arms = self.gen_matches_arms(info);
        let to_owned_arms = self.gen_to_owned_arms(info, TypeVariant::Ref);

        quote! {
            impl<'a> #ref_name<'a> {
                #[inline]
                pub fn to_owned(&self) -> #name {
                    match self {
                        #(#to_owned_arms,)*
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
                        #(#size_arms,)*
                    }
                }

                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    self.kind().is_fixed_size()
                }

                #[inline]
                pub fn is_null(&self) -> bool {
                    self.kind().is_null()
                }

                pub fn matches(&self, other: #kind_name) -> bool {
                    if self.is_null() {
                        return true;
                    }
                    match (self, other) {
                        #(#matches_arms,)*
                        _ => false,
                    }
                }
            }
        }
    }

    fn gen_ref_mut_impl(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;
        let ref_mut_name = info.ref_mut_name();
        let kind_name = info.kind_name();

        let kind_arms = self.gen_kind_arms(info);
        let size_arms = self.gen_size_arms(info, TypeVariant::RefMut);
        let matches_arms = self.gen_matches_arms(info);
        let to_owned_arms = self.gen_to_owned_arms(info, TypeVariant::RefMut);

        quote! {
            impl<'a> #ref_mut_name<'a> {
                #[inline]
                pub fn to_owned(&self) -> #name {
                    match self {
                        #(#to_owned_arms,)*
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
                        #(#size_arms,)*
                    }
                }

                #[inline]
                pub fn is_fixed_size(&self) -> bool {
                    self.kind().is_fixed_size()
                }

                #[inline]
                pub fn is_null(&self) -> bool {
                    self.kind().is_null()
                }

                pub fn matches(&self, other: #kind_name) -> bool {
                    if self.is_null() {
                        return true;
                    }
                    match (self, other) {
                        #(#matches_arms,)*
                        _ => false,
                    }
                }
            }
        }
    }
}

struct ArithmeticGenerator;

impl Generator for ArithmeticGenerator {
    fn generate(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;

        let checked_methods = self.gen_checked_methods(info);
        let trait_impls = self.gen_trait_impls(info);

        quote! {
            impl #name {
                #checked_methods
            }

            #trait_impls
        }
    }
}

impl ArithmeticGenerator {
    fn gen_checked_methods(&self, info: &EnumInfo) -> TokenStream {
        let methods: Vec<_> = ARITHMETIC_OPS
            .iter()
            .map(|(method_name, trait_name, op_name)| {
                self.gen_checked_method(info, method_name, trait_name, op_name)
            })
            .collect();

        quote! { #(#methods)* }
    }

    fn gen_checked_method(
        &self,
        info: &EnumInfo,
        method_name: &str,
        trait_name: &str,
        op_name: &str,
    ) -> TokenStream {
        let method_ident = format_ident!("{}", method_name);
        let trait_ident = format_ident!("{}", trait_name);
        let op_ident = format_ident!("{}", op_name);

        let match_arms: Vec<_> = info
            .variants
            .iter()
            .map(|v| {
                let vn = &v.name;
                if v.inner_type.is_some() && v.is_arith() {
                    quote! {
                        (Self::#vn(a), Self::#vn(b)) => {
                            Ok(Self::#vn(std::ops::#trait_ident::#op_ident(*a, *b)))
                        }
                    }
                } else if v.inner_type.is_some() {
                    quote! { (Self::#vn(_), Self::#vn(_)) => Err(crate::database::errors::TypeError::NullOperation) }
                } else {
                    quote! { (Self::#vn, Self::#vn) => Err(crate::database::errors::TypeError::NullOperation) }
                }
            })
            .collect();

        quote! {
            /// Perform checked arithmetic, returning error on type mismatch.
            pub fn #method_ident(&self, rhs: &Self) -> Result<Self, crate::database::errors::TypeError> {
                match (self, rhs) {
                    #(#match_arms,)*
                    (left, right) => Err(crate::database::errors::TypeError::TypeMismatch {
                        left: left.kind(),
                        right: right.kind(),
                    }),
                }
            }
        }
    }

    fn gen_trait_impls(&self, info: &EnumInfo) -> TokenStream {
        let name = &info.name;

        quote! {
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

            impl<'a> std::ops::Add<#name> for &'a #name {
                type Output = #name;
                fn add(self, rhs: #name) -> Self::Output {
                    self.checked_add(&rhs).expect("arithmetic add failed")
                }
            }

            impl<'a, 'b> std::ops::Add<&'b #name> for &'a #name {
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

            impl<'a> std::ops::Sub<#name> for &'a #name {
                type Output = #name;
                fn sub(self, rhs: #name) -> Self::Output {
                    self.checked_sub(&rhs).expect("arithmetic sub failed")
                }
            }

            impl<'a, 'b> std::ops::Sub<&'b #name> for &'a #name {
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

            impl<'a> std::ops::Mul<#name> for &'a #name {
                type Output = #name;
                fn mul(self, rhs: #name) -> Self::Output {
                    self.checked_mul(&rhs).expect("arithmetic mul failed")
                }
            }

            impl<'a, 'b> std::ops::Mul<&'b #name> for &'a #name {
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

            impl<'a> std::ops::Div<#name> for &'a #name {
                type Output = #name;
                fn div(self, rhs: #name) -> Self::Output {
                    self.checked_div(&rhs).expect("arithmetic div failed")
                }
            }

            impl<'a, 'b> std::ops::Div<&'b #name> for &'a #name {
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

            impl<'a> std::ops::Rem<#name> for &'a #name {
                type Output = #name;
                fn rem(self, rhs: #name) -> Self::Output {
                    self.checked_rem(&rhs).expect("arithmetic rem failed")
                }
            }

            impl<'a, 'b> std::ops::Rem<&'b #name> for &'a #name {
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

    let kind_enum = KindEnumGenerator.generate(&info);
    let ref_enums = RefEnumsGenerator.generate(&info);
    let casts = CastGenerator.generate(&info);
    let comparisons = ComparisonGenerator.generate(&info);
    let conversions = ConversionGenerator.generate(&info);
    let impl_blocks = ImplBlockGenerator.generate(&info);
    let arithmetic = ArithmeticGenerator.generate(&info);
    let functions = OpsGenerator.generate(&info);
    let cls = NumericTypeGenerator.generate(&info);

    quote! {
        #kind_enum
        #ref_enums
        #casts
        #comparisons
        #conversions
        #functions
        #impl_blocks
        #arithmetic
        #cls
    }
}
