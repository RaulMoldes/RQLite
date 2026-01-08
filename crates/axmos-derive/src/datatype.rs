use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    Attribute, Data, DeriveInput, Error as SynError, Fields, Ident, LitStr, Result as SynResult,
    Type, parse2,
};

use std::{collections::HashSet, slice::Iter};

/// Converts an [String] input from pascal case [PascalCase] to snake case [snakeCase]
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
        } else {
            result.push(c);
        }
    }
    result
}

/// Trait which includes the reference to the [Ref::Type] for the main enum
#[derive(Debug)]
pub struct TypeConfig {
    pub ref_trait_path: TokenStream, // Path to the trait that has the [::Ref] implementation
    pub owned_trait_path: TokenStream,
}

impl TypeConfig {
    pub fn ref_trait_path(&self) -> &TokenStream {
        &self.ref_trait_path
    }

    pub fn owned_trait_path(&self) -> &TokenStream {
        &self.owned_trait_path
    }

    fn parse(attrs: &[Attribute]) -> SynResult<Self> {
        let Some(attr) = attrs.iter().find(|a| a.path().is_ident("config")) else {
            return Err(SynError::new(
                proc_macro2::Span::call_site(),
                "no datatype configuration provided".to_string(),
            ));
        };

        let mut ref_trait_path = None;
        let mut owned_trait = None;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("reference_trait") {
                let v: LitStr = meta.value()?.parse()?;
                ref_trait_path = Some(v.parse::<TokenStream>()?);
            } else if meta.path.is_ident("owned_trait") {
                let v: LitStr = meta.value()?.parse()?;
                owned_trait = Some(v.parse::<TokenStream>()?);
            }
            Ok(())
        })?;

        Ok(Self {
            ref_trait_path: ref_trait_path
                .ok_or_else(|| SynError::new_spanned(attr, "missing 'reference_trait'"))?,

            owned_trait_path: owned_trait
                .ok_or_else(|| SynError::new_spanned(attr, "missing 'owned_trait'"))?,
        })
    }
}

// Type of delegation
// Can be either const or method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelegateKind {
    Method,
    Const,
}

// Delegation target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DelegateTarget {
    #[default]
    Main, // Main implementation
    Kind, // Kind enum implementation
    Ref,  // Reference enum implementation.
}

// Indicator to determine whether we have to wrap th eresult over the ref enum or over the main enum (for some methods like reinterpret cast)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WrapVariant {
    is_result: bool,
    is_tuple: bool,
    target: DelegateTarget,
}

// Delegate attribute representation
#[derive(Debug)]
pub struct DelegateAttr {
    // Path to the deleagated trait
    pub trait_path: TokenStream,

    // Name of the delegated method
    pub name: Ident,

    // If the method has to be renamed.
    pub rename: Option<Ident>,

    // Method return type.
    pub return_type: TokenStream,

    // Method default value.
    pub default_value: TokenStream,

    // THe type of delegate (method or const)
    pub kind: DelegateKind,

    /// The delegated target (Kind enum, ref enum or main enum)
    pub target: DelegateTarget,

    // Any custom args.
    pub args: Option<TokenStream>,

    /// Filter the implementation of the trait for spedific variants attrs.
    pub filter_attributes: Option<String>,

    // Call args
    pub call_args: Option<TokenStream>,

    /// WHether or not to wrap the result
    pub wrap_variant: Option<WrapVariant>,

    // Generic parameters for the method
    pub generics: Option<TokenStream>,

    // Where clause for generics
    pub where_clause: Option<TokenStream>,
}

impl DelegateAttr {
    /// Gets the path of the dispatched trait.
    /// For example [core::types::Hashable]
    pub fn trait_path(&self) -> &TokenStream {
        &self.trait_path
    }

    /// The original name of the method in the source trait
    pub fn original_name(&self) -> &Ident {
        &self.name
    }

    /// THe return type of the function/const.
    pub fn return_type(&self) -> &TokenStream {
        &self.return_type
    }

    /// THe default value of the function/const.
    pub fn default(&self) -> &TokenStream {
        &self.default_value
    }

    pub fn target_enum(&self) -> &DelegateTarget {
        &self.target
    }

    pub fn delegate_type(&self) -> &DelegateKind {
        &self.kind
    }

    pub fn custom_args(&self) -> Option<&TokenStream> {
        self.args.as_ref()
    }

    pub fn call_args(&self) -> Option<&TokenStream> {
        self.call_args.as_ref()
    }

    // Obtain the generated name.
    // If rename is None, will use the standard name by default.
    fn generated_name(&self) -> &Ident {
        self.rename.as_ref().unwrap_or(&self.name)
    }

    fn parse(attr: &Attribute) -> SynResult<Option<Self>> {
        if !attr.path().is_ident("delegate") {
            return Ok(None);
        }

        let mut trait_path = None;
        let mut name = None;
        let mut rename = None;
        let mut return_type = None;
        let mut default_value = None;
        let mut kind = DelegateKind::Method;
        let mut target = DelegateTarget::Main;
        let mut wrap_variant = None;
        let mut args = None;
        let mut call_args = None;
        let mut generics = None;
        let mut where_clause = None;
        let mut filter_attributes = None;

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("trait") {
                let v: LitStr = meta.value()?.parse()?;
                trait_path = Some(v.parse::<TokenStream>()?);
            } else if meta.path.is_ident("method") {
                let v: LitStr = meta.value()?.parse()?;
                name = Some(format_ident!("{}", v.value()));
                kind = DelegateKind::Method;
            } else if meta.path.is_ident("const") {
                let v: LitStr = meta.value()?.parse()?;
                name = Some(format_ident!("{}", v.value()));
                kind = DelegateKind::Const;
            } else if meta.path.is_ident("as") {
                let v: LitStr = meta.value()?.parse()?;
                rename = Some(format_ident!("{}", v.value()));
            } else if meta.path.is_ident("return_type") {
                let v: LitStr = meta.value()?.parse()?;
                return_type = Some(v.parse::<TokenStream>()?);
            } else if meta.path.is_ident("default") {
                let v: LitStr = meta.value()?.parse()?;
                default_value = Some(v.parse::<TokenStream>()?);
            } else if meta.path.is_ident("target") {
                let v: LitStr = meta.value()?.parse()?;
                target = match v.value().as_str() {
                    "main" => DelegateTarget::Main,
                    "kind" => DelegateTarget::Kind,
                    "ref" => DelegateTarget::Ref,
                    _ => return Err(meta.error("target must be 'main', 'kind', or 'ref'")),
                };
            } else if meta.path.is_ident("filter_attributes") {
                let v: LitStr = meta.value()?.parse()?;
                filter_attributes = Some(v.value());
            } else if meta.path.is_ident("wrap_variant") {
                let v: LitStr = meta.value()?.parse()?;
                let value = v.value();
                // Parse format: "main" or "main,result" or "main,tuple" or "main,result,tuple"
                let parts: Vec<&str> = value.split(',').map(|s| s.trim()).collect();

                let target = match parts.get(0) {
                    Some(&"main") => DelegateTarget::Main,
                    Some(&"ref") => DelegateTarget::Ref,
                    _ => return Err(meta.error("wrap_variant target must be 'main' or 'ref'")),
                };

                let is_result = parts.iter().any(|&p| p == "result");
                let is_tuple = parts.iter().any(|&p| p == "tuple");

                wrap_variant = Some(WrapVariant {
                    target,
                    is_result,
                    is_tuple,
                });
            } else if meta.path.is_ident("args") {
                let v: LitStr = meta.value()?.parse()?;
                args = Some(v.parse::<TokenStream>()?);
            } else if meta.path.is_ident("call_args") {
                let v: LitStr = meta.value()?.parse()?;
                call_args = Some(v.parse::<TokenStream>()?);
            } else if meta.path.is_ident("generics") {
                let v: LitStr = meta.value()?.parse()?;
                generics = Some(v.parse::<TokenStream>()?);
            } else if meta.path.is_ident("where_clause") {
                let v: LitStr = meta.value()?.parse()?;
                where_clause = Some(v.parse::<TokenStream>()?);
            }
            Ok(())
        })?;

        Ok(Some(Self {
            trait_path: trait_path.ok_or_else(|| SynError::new_spanned(attr, "missing 'trait'"))?,
            name: name.ok_or_else(|| SynError::new_spanned(attr, "missing 'method' or 'const'"))?,
            rename,
            return_type: return_type
                .ok_or_else(|| SynError::new_spanned(attr, "missing 'return_type'"))?,
            default_value: default_value
                .ok_or_else(|| SynError::new_spanned(attr, "missing 'default'"))?,

            kind,
            target,
            args,
            call_args,
            wrap_variant,
            generics,
            where_clause,
            filter_attributes,
        }))
    }
}

/// Identified which is the null variant of the enum
///
/// There must be only one.
#[derive(Debug, Default)]
pub struct VariantAttrs {
    pub is_null: bool,
    pub is_numeric: bool,
}

impl VariantAttrs {
    fn parse(attrs: &[Attribute]) -> Self {
        Self {
            is_null: attrs.iter().any(|a| a.path().is_ident("null")),
            is_numeric: attrs.iter().any(|a| a.path().is_ident("numeric")),
        }
    }

    pub fn has_attr(&self, name: &str) -> bool {
        match name {
            "null" => self.is_null,
            "numeric" => self.is_numeric,
            _ => false,
        }
    }

    /// Whether the #[is_null] attribute is set for this target variant.
    pub fn is_null(&self) -> bool {
        self.is_null
    }

    /// Whether the #[is_null] attribute is set for this target variant.
    pub fn is_numeric(&self) -> bool {
        self.is_numeric
    }
}

/// Information to track about each enum's variant.
pub struct VariantInfo {
    // Name of the variant
    pub name: Ident,
    // Inner type if set
    pub inner_type: Option<Type>,
    // Variant parsed attributes.
    pub attrs: VariantAttrs,
}

impl VariantInfo {
    pub fn name(&self) -> &Ident {
        &self.name
    }

    pub fn inner_type(&self) -> Option<&Type> {
        self.inner_type.as_ref()
    }

    pub fn is_null(&self) -> bool {
        self.attrs.is_null()
    }

    pub fn is_numeric(&self) -> bool {
        self.attrs.is_numeric()
    }
}

/// Information to track about the global enum.
pub struct EnumInfo {
    // Name of the parsed enum
    pub name: Ident,

    // List of parsed variants
    pub variants: Vec<VariantInfo>,

    // List of delegates
    pub delegates: Vec<DelegateAttr>,

    // Delegate reference information
    pub datatype_config: TypeConfig,
}

impl EnumInfo {
    pub fn try_from_input(input: DeriveInput) -> SynResult<Self> {
        // Parse the input as an enum and return a compile error if it cannot.
        let Data::Enum(data) = input.data else {
            return Err(SynError::new_spanned(input.ident, "Only enums supported"));
        };

        // parse the delegate attributes.
        let delegates = input
            .attrs
            .iter()
            .filter_map(|a| DelegateAttr::parse(a).transpose())
            .collect::<SynResult<Vec<_>>>()?;

        // Parse the delegate_ref attribute
        let config = TypeConfig::parse(&input.attrs)?;

        // Track how many [is_null] we find.
        let mut is_null_flag: bool = false;

        // Parse each variant.
        let variants = data
            .variants
            .into_iter()
            .map(|v| {
                let inner_type = match v.fields {
                    Fields::Unnamed(f) if f.unnamed.len() == 1 => {
                        Some(f.unnamed.into_iter().next().unwrap().ty)
                    }
                    Fields::Unit => None,
                    _ => return Err(SynError::new_spanned(v.ident, "Invalid variant")),
                };

                let attrs = VariantAttrs::parse(&v.attrs);

                // We only can deal with one null variant per Autogenerated type enum.
                if attrs.is_null() && is_null_flag {
                    return Err(SynError::new_spanned(
                        v.ident,
                        "Invalid variant attribut. Only one null marker is allowed.",
                    ));
                } else if attrs.is_null() {
                    is_null_flag = true;
                };

                Ok(VariantInfo {
                    name: v.ident,
                    inner_type,
                    attrs,
                })
            })
            .collect::<SynResult<Vec<_>>>()?;

        Ok(Self {
            name: input.ident,
            variants,
            delegates,
            datatype_config: config,
        })
    }
    pub fn name(&self) -> &Ident {
        &self.name
    }

    pub fn kind_name(&self) -> Ident {
        format_ident!("{}Kind", self.name)
    }
    pub fn ref_name(&self) -> Ident {
        format_ident!("{}Ref", self.name)
    }

    pub fn iter_variants(&self) -> Iter<'_, VariantInfo> {
        self.variants.iter()
    }

    pub fn config(&self) -> &TypeConfig {
        &self.datatype_config
    }

    pub fn iter_delegates(&self) -> Iter<'_, DelegateAttr> {
        self.delegates.iter()
    }

    // Whether this enum has delegates to process.
    pub fn has_delegates(&self) -> bool {
        !self.delegates.is_empty()
    }
}

/// Entry point to the macro.
///
/// Generates the data type implementation
pub fn autogen_data_type_impl(input: TokenStream) -> TokenStream {
    let input = match parse2::<DeriveInput>(input) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error(),
    };

    let info = match EnumInfo::try_from_input(input) {
        Ok(i) => i,
        Err(e) => return e.to_compile_error(),
    };

    let kind_enum = autogen_kind_enum(&info);
    let ref_enum = autogen_ref_enum(&info);
    let base_impl = autogen_base(&info);
    let delegate_impls = autogen_delegates(&info);
    let display_impl = gen_display(&info);
    let from_impls = gen_from_impls(&info);

    quote! {
        #kind_enum
        #ref_enum
        #base_impl
        #delegate_impls
        #display_impl
        #from_impls
    }
}

// Generates the Kind enum using type attributes.
fn autogen_kind_enum(info: &EnumInfo) -> TokenStream {
    let kind_name = info.kind_name();
    let variants: Vec<_> = info.variants.iter().map(|v| &v.name).collect();
    let indices: Vec<u8> = (0..variants.len() as u8).collect();

    // Generates the [is_null] for the [null] variant.
    let is_null_arms: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            let is_null = v.is_null();
            quote! { Self::#vn => #is_null }
        })
        .collect();

    let is_numeric_arms: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            let is_null = v.is_numeric();
            quote! { Self::#vn => #is_null }
        })
        .collect();

    // Generates one method to match for each variant.
    let is_type_methods: Vec<_> = info
        .iter_variants()
        .filter(|p| !p.is_null())
        .map(|v| {
            let vn = v.name();
            let method = format_ident!("is_{}", to_snake_case(&vn.to_string()));
            quote! {
                #[inline] pub const fn #method(self) -> bool { matches!(self, Self::#vn) }
            }
        })
        .collect();

    quote! {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(u8)]
        pub enum #kind_name { #(#variants = #indices,)* }

        impl #kind_name {
            #[inline] pub const fn name(self) -> &'static str {
                match self { #(Self::#variants => stringify!(#variants),)* }
            }
            #[inline] pub const fn is_null(self) -> bool { match self { #(#is_null_arms,)* } }
            #[inline] pub const fn is_numeric(self) -> bool { match self { #(#is_numeric_arms,)* } }
            #[inline] pub fn from_repr(v: u8) -> Option<Self> {
                match v { #(#indices => Some(Self::#variants),)* _ => None }
            }
            #[inline] pub const fn as_u8(self) -> u8 { self as u8 }
            #(#is_type_methods)*
        }

        impl std::fmt::Display for #kind_name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.name())
            }
        }
        impl From<#kind_name> for u8 { fn from(v: #kind_name) -> u8 { v as u8 } }
        impl TryFrom<u8> for #kind_name {
            type Error = ();
            fn try_from(v: u8) -> Result<Self, ()> { Self::from_repr(v).ok_or(()) }
        }
    }
}

/// Autogenerates the reference implementation using the [delegate_ref] attr
/// if it is set.
fn autogen_ref_enum(info: &EnumInfo) -> TokenStream {
    let config = info.config();
    let ref_name = info.ref_name();
    let kind_name = info.kind_name();

    let ref_trait_path = config.ref_trait_path();
    let owned_trait_path = config.owned_trait_path();

    let variants: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            if let Some(ty) = v.inner_type() {
                quote! { #vn(<#ty as #owned_trait_path>::Ref<'a>) }
            } else {
                quote! { #vn }
            }
        })
        .collect();

    let kind_arms: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            if v.inner_type().is_some() {
                quote! { Self::#vn(_) => #kind_name::#vn }
            } else {
                quote! { Self::#vn => #kind_name::#vn }
            }
        })
        .collect();
    let main_name = info.name();

    let to_owned_arms: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            if let Some(ty) = v.inner_type() {
                quote! { Self::#vn(inner) => Some(#main_name::#vn(<<#ty as #owned_trait_path>::Ref<'a> as #ref_trait_path>::to_owned(inner))) }
            } else {
                quote! { Self::#vn => None }
            }
        })
        .collect();

    // Generates to_f64 arms for numeric variants
    let to_f64_arms: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            if v.is_numeric() {
                if v.inner_type().is_some() {
                    quote! { Self::#vn(inner) => Some(inner.0 as f64) }
                } else {
                    quote! { Self::#vn => None }
                }
            } else {
                if v.inner_type().is_some() {
                    quote! { Self::#vn(_) => None }
                } else {
                    quote! { Self::#vn => None }
                }
            }
        })
        .collect();

    let as_bytes_arms: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            if let Some(ty) = v.inner_type() {
                quote! { Self::#vn(inner) => <<#ty as #owned_trait_path>::Ref<'a> as #ref_trait_path>::as_bytes(inner) }
            } else {
                quote! { Self::#vn => &[] }
            }
        })
        .collect();

    quote! {
        #[derive(Debug, Clone, Copy)]
        pub enum #ref_name<'a> { #(#variants,)* }

        impl<'a> #ref_name<'a> {
            #[inline] pub fn kind(&self) -> #kind_name { match self { #(#kind_arms,)* } }
            #[inline] pub fn is_null(&self) -> bool { self.kind().is_null() }
            #[inline] pub fn is_numeric(&self) -> bool { self.kind().is_numeric() }
            #[inline] pub fn to_owned(&self) -> Option<#main_name> { match self {#(#to_owned_arms,)*}  }

             #[inline] pub fn to_f64(&self) -> Option<f64> { match self {#(#to_f64_arms,)*}  }
            #[inline] pub fn as_bytes(&self) -> &[u8] { match self {#(#as_bytes_arms,)*}  }
        }
    }
}

fn autogen_base(info: &EnumInfo) -> TokenStream {
    let name = info.name();
    let kind_name = info.kind_name();

    let kind_arms: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            if v.inner_type.is_some() {
                quote! { Self::#vn(_) => #kind_name::#vn }
            } else {
                quote! { Self::#vn => #kind_name::#vn }
            }
        })
        .collect();

    // Generates to_f64 arms for numeric variants
    let to_f64_arms: Vec<_> = info
        .iter_variants()
        .map(|v| {
            let vn = v.name();
            if v.is_numeric() {
                if v.inner_type().is_some() {
                    quote! { Self::#vn(inner) => Some(inner.0 as f64) }
                } else {
                    quote! { Self::#vn => None }
                }
            } else {
                if v.inner_type().is_some() {
                    quote! { Self::#vn(_) => None }
                } else {
                    quote! { Self::#vn => None }
                }
            }
        })
        .collect();

    // Generates a method for each variant to identify the autogenerated enum
    //
    // Note that we skip the [null] variant in order not to collide with the [is_null] method that we automatically generate.
    // I do not think anyone would be interested in calling a non-null variant null, xddd
    let is_methods: Vec<_> = info
        .iter_variants()
        .filter(|v| !v.is_null())
        .map(|v| {
            let vn = v.name();
            let method = format_ident!("is_{}", to_snake_case(&vn.to_string()));
            let pat = if v.inner_type().is_some() {
                quote! { Self::#vn(_) }
            } else {
                quote! { Self::#vn }
            };
            quote! { #[inline] pub const fn #method(&self) -> bool { matches!(self, #pat) } }
        })
        .collect();

    // Generates methods to automatically convert from the enum to any of its variants.
    let as_methods: Vec<_> = info
        .iter_variants()
        .filter_map(|v| {
            let vn = v.name();
            let ty = v.inner_type()?;
            let method = format_ident!("as_{}", to_snake_case(&vn.to_string()));
            Some(quote! {
                #[inline] pub fn #method(&self) -> Option<&#ty> {
                    match self { Self::#vn(inner) => Some(inner), _ => None }
                }
            })
        })
        .collect();

    quote! {
        impl #name {
            #[inline] pub fn kind(&self) -> #kind_name { match self { #(#kind_arms,)* } }
            #[inline] pub fn is_null(&self) -> bool { self.kind().is_null() }
            #[inline] pub fn is_numeric(&self) -> bool { self.kind().is_numeric() }
            #[inline] pub fn is_some(&self) -> bool { !self.is_null() }
            /// Converts numeric variants to f64 for comparisons.
            /// Returns None for non-numeric types.
            #[inline]
            pub fn to_f64(&self) -> Option<f64> {
                match self { #(#to_f64_arms,)* }
            }
            #(#is_methods)*
            #(#as_methods)*
        }
    }
}

/// Iterates over the list of delegates and generates a function call for the target trait.
fn autogen_delegates(info: &EnumInfo) -> TokenStream {
    if !info.has_delegates() {
        return quote! {};
    }

    let name = info.name();
    let kind_name = info.kind_name();
    let ref_name = info.ref_name();

    let mut main = Vec::new();
    let mut kind = Vec::new();
    let mut ref_m = Vec::new();

    for d in info.iter_delegates() {
        let original_name = d.original_name();
        let method = d.generated_name();
        let ret = d.return_type();
        let default = d.default();
        let trait_path = d.trait_path();
        let custom_args = d
            .custom_args()
            .map(|a| quote! { #a })
            .unwrap_or(quote! { &self });
        let target = *d.target_enum();
        let generics = d
            .generics
            .as_ref()
            .map(|g| quote! { <#g> })
            .unwrap_or(quote! {});
        let where_clause = d
            .where_clause
            .as_ref()
            .map(|w| quote! { where #w })
            .unwrap_or(quote! {});

        // Dispatch a delegation for each variant.
        let arms: Vec<_> = info
            .iter_variants()
            .map(|v| {
                let vn = v.name();

                // Check if this variant should use the delegate or the default
                let should_delegate = if let Some(ref attr_filter) = d.filter_attributes {
                    v.attrs.has_attr(attr_filter)
                } else {
                    true // No filter, delegate for all variants with inner type
                };

                if let Some(ty) = v.inner_type() {
                    if should_delegate {
                        match *d.delegate_type() {
                            DelegateKind::Const => {
                                let pat = if target == DelegateTarget::Kind {
                                    quote! { Self::#vn }
                                } else if v.inner_type().is_some() {
                                    quote! { Self::#vn(inner) }
                                } else {
                                    quote! { Self::#vn }
                                };
                                quote! { #pat => <#ty as #trait_path>::#original_name }
                            }
                            DelegateKind::Method => {
                                let call_expr = if let Some(ca) = d.call_args() {
                                    quote! { <#ty as #trait_path>::#original_name(#ca) }
                                } else if target == DelegateTarget::Ref {
                                    quote! { #trait_path::#original_name(inner) }
                                } else {
                                    // REVISAR AQUI
                                    quote! { <#ty as #trait_path>::#original_name(inner) }
                                };

                                let pat = if target == DelegateTarget::Kind {
                                    quote! { Self::#vn }
                                } else {
                                    quote! { Self::#vn(inner) }
                                };

                                if let Some(ref variant) = d.wrap_variant {
                                    match variant.target {
                                        DelegateTarget::Ref => {
                                            if variant.is_tuple {
                                                if variant.is_result {
                                                    quote! {
                                                        #pat => {
                                                            let (r, sz) = #call_expr?;
                                                            Ok((#ref_name::#vn(r), sz))
                                                        }
                                                    }
                                                } else {
                                                    quote! {
                                                        #pat => {
                                                            let (r, sz) = #call_expr;
                                                            (#ref_name::#vn(r), sz)
                                                        }
                                                    }
                                                }
                                            } else {
                                                if variant.is_result {
                                                    quote! {
                                                        #pat => {
                                                            let r = #call_expr?;
                                                            Ok(#ref_name::#vn(r))
                                                        }
                                                    }
                                                } else {
                                                    quote! {
                                                        #pat => #ref_name::#vn(#call_expr)
                                                    }
                                                }
                                            }
                                        }
                                        DelegateTarget::Main => {
                                            if variant.is_tuple {
                                                if variant.is_result {
                                                    quote! {
                                                        #pat => {
                                                            let (r, sz) = #call_expr?;
                                                            Ok((#name::#vn(r), sz))
                                                        }
                                                    }
                                                } else {
                                                    quote! {
                                                        #pat => {
                                                            let (r, sz) = #call_expr;
                                                            (#name::#vn(r), sz)
                                                        }
                                                    }
                                                }
                                            } else {
                                                if variant.is_result {
                                                    quote! {
                                                        #pat => {
                                                            let r = #call_expr?;
                                                            Ok(#name::#vn(r))
                                                        }
                                                    }
                                                } else {
                                                    quote! {
                                                        #pat => #name::#vn(#call_expr)
                                                    }
                                                }
                                            }
                                        }
                                        DelegateTarget::Kind => {
                                            quote! { #pat => #call_expr }
                                        }
                                    }
                                } else {
                                    quote! { #pat => #call_expr }
                                }
                            }
                        }
                    } else {
                        let pat = quote! { Self::#vn(_) };
                        quote! { #pat => #default }
                    }
                } else {
                    quote! { Self::#vn => #default }
                }
            })
            .collect();

        let m = quote! {
            #[inline]
            pub fn #method #generics (#custom_args) -> #ret #where_clause {
                match self { #(#arms,)* }
            }
        };

        match target {
            DelegateTarget::Main => main.push(m),
            DelegateTarget::Kind => kind.push(m),
            DelegateTarget::Ref => ref_m.push(m),
        }
    }

    // dispatch the collected delegates of each enum variant.
    let main_impl = if main.is_empty() {
        quote! {}
    } else {
        quote! { impl #name { #(#main)* } }
    };
    let kind_impl = if kind.is_empty() {
        quote! {}
    } else {
        quote! { impl #kind_name { #(#kind)* } }
    };
    let ref_impl = if ref_m.is_empty() {
        quote! {}
    } else {
        quote! { impl<'a> #ref_name<'a> { #(#ref_m)* } }
    };

    quote! { #main_impl #kind_impl #ref_impl }
}

/// Generates a [std::fmt::Display] implementation fot the enum by means of dispatching to he implementation for the target variant.
fn gen_display(info: &EnumInfo) -> TokenStream {
    let name = &info.name;
    let arms: Vec<_> = info
        .variants
        .iter()
        .map(|v| {
            let vn = &v.name;
            if v.inner_type.is_some() {
                quote! { Self::#vn(inner) => std::fmt::Display::fmt(inner, f) }
            } else {
                let upper = vn.to_string().to_uppercase();
                quote! { Self::#vn => write!(f, #upper) }
            }
        })
        .collect();

    quote! {
        impl std::fmt::Display for #name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self { #(#arms,)* }
            }
        }
    }
}

/// Generates [From<_>] implementation for the enum for each of the inner types.
fn gen_from_impls(info: &EnumInfo) -> TokenStream {
    let name = info.name();

    // Track types we've already generated From for to avoid duplicates
    let mut seen_types = HashSet::new();

    let impls: Vec<_> = info
        .variants
        .iter()
        .filter_map(|v| {
            let vn = v.name();
            let ty = v.inner_type()?;

            // Convert type to string for deduplication
            let ty_str = quote!(#ty).to_string();
            if !seen_types.insert(ty_str) {
                return None; // Already generated From for this type
            }

            Some(quote! { impl From<#ty> for #name { fn from(v: #ty) -> Self { Self::#vn(v) } } })
        })
        .collect();
    quote! { #(#impls)* }
}
