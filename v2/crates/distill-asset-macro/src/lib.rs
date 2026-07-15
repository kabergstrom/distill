//! `#[asset]` generates the §4 runtime descriptor, §12 live native
//! tree and callback tables, deterministic encoder, logical hash walk,
//! and structural default table for an asset record.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    parse_macro_input, Attribute, Expr, ExprLit, Fields, Ident, Item, ItemEnum, ItemStruct, Lit,
    Meta, Token, Type,
};
use unicode_normalization::UnicodeNormalization;

#[derive(Default)]
struct TypeArgs {
    uuid: Option<(String, Span)>,
    rev: Option<(u32, Span)>,
    build_only: bool,
}

#[derive(Default, Clone, Copy)]
struct NodeArgs {
    skip: bool,
    blob: bool,
    tag: bool,
    rev: u32,
    rev_present: bool,
}

#[proc_macro_attribute]
pub fn asset(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match parse_type_args(attr.into()) {
        Ok(args) => args,
        Err(error) => return error.into_compile_error().into(),
    };
    let item = parse_macro_input!(item as Item);
    match expand(args, item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

fn parse_type_args(tokens: TokenStream2) -> syn::Result<TypeArgs> {
    let metas = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(tokens)?;
    let mut args = TypeArgs::default();
    for meta in metas {
        match meta {
            Meta::NameValue(value) if value.path.is_ident("uuid") => {
                if args.uuid.is_some() {
                    return Err(syn::Error::new_spanned(value, "duplicate `uuid`"));
                }
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(value_lit),
                    ..
                }) = value.value
                else {
                    return Err(syn::Error::new_spanned(
                        value,
                        "`uuid` must be a string literal",
                    ));
                };
                args.uuid = Some((value_lit.value(), value_lit.span()));
            }
            Meta::NameValue(value) if value.path.is_ident("rev") => {
                if args.rev.is_some() {
                    return Err(syn::Error::new_spanned(value, "duplicate `rev`"));
                }
                let span = value.path.segments[0].ident.span();
                args.rev = Some((parse_u32_expr(&value.value, "`rev`")?, span));
            }
            Meta::Path(path) if path.is_ident("build_only") => {
                if args.build_only {
                    return Err(syn::Error::new_spanned(path, "duplicate `build_only`"));
                }
                args.build_only = true;
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "unknown asset type option; expected `uuid`, `rev`, or `build_only`",
                ));
            }
        }
    }
    Ok(args)
}

fn parse_u32_expr(expr: &Expr, label: &str) -> syn::Result<u32> {
    let Expr::Lit(ExprLit {
        lit: Lit::Int(value),
        ..
    }) = expr
    else {
        return Err(syn::Error::new_spanned(
            expr,
            format!("{label} must be a u32 literal"),
        ));
    };
    value
        .base10_parse::<u32>()
        .map_err(|_| syn::Error::new_spanned(value, format!("{label} must fit u32")))
}

fn expand(args: TypeArgs, item: Item) -> syn::Result<TokenStream2> {
    let (uuid, uuid_span) = args
        .uuid
        .as_ref()
        .ok_or_else(|| syn::Error::new(Span::call_site(), "`#[asset]` requires `uuid = \"…\"`"))?;
    let uuid = parse_uuid(uuid).ok_or_else(|| {
        syn::Error::new(
            *uuid_span,
            "malformed asset UUID; expected RFC 4122 hyphenated hexadecimal form",
        )
    })?;
    match item {
        Item::Struct(item) => expand_struct(args, uuid, item),
        Item::Enum(item) => expand_enum(args, uuid, item),
        other => Err(syn::Error::new_spanned(
            other,
            "`#[asset]` supports structs and enums only",
        )),
    }
}

fn validate_item(ident: &Ident, generics: &syn::Generics) -> syn::Result<()> {
    if !generics.params.is_empty() || generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            generics,
            "generic asset declarations are not supported: one TypeUuid must identify one concrete layout",
        ));
    }
    if ident.to_string().starts_with("__distill") {
        return Err(syn::Error::new_spanned(
            ident,
            "asset name uses a reserved codegen prefix",
        ));
    }
    Ok(())
}

struct FieldInfo {
    member: syn::Member,
    binding: Ident,
    name: String,
    ty: Type,
    args: NodeArgs,
    declaration: u32,
}

fn fields_info(fields: &mut Fields) -> syn::Result<Vec<FieldInfo>> {
    let mut out = Vec::new();
    for (index, field) in fields.iter_mut().enumerate() {
        let args = take_node_args(&mut field.attrs, false)?;
        validate_field_args(&field.ty, args)?;
        let (member, binding, name) = match &field.ident {
            Some(ident) => (
                syn::Member::Named(ident.clone()),
                ident.clone(),
                ident.to_string(),
            ),
            None => (
                syn::Member::Unnamed(syn::Index::from(index)),
                format_ident!("__distill_field_{index}"),
                index.to_string(),
            ),
        };
        out.push(FieldInfo {
            member,
            binding,
            name,
            ty: field.ty.clone(),
            args,
            declaration: index as u32,
        });
    }
    Ok(out)
}

fn take_node_args(attrs: &mut Vec<Attribute>, variant: bool) -> syn::Result<NodeArgs> {
    let mut result = NodeArgs::default();
    let mut retained = Vec::with_capacity(attrs.len());
    for attr in attrs.drain(..) {
        if !attr.path().is_ident("asset") {
            retained.push(attr);
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rev") {
                if result.rev_present {
                    return Err(meta.error("duplicate `rev`"));
                }
                let value: syn::LitInt = meta.value()?.parse()?;
                result.rev = value
                    .base10_parse::<u32>()
                    .map_err(|_| meta.error("`rev` must fit u32"))?;
                result.rev_present = true;
                return Ok(());
            }
            if variant {
                return Err(meta.error("enum variants support only `#[asset(rev = N)]`"));
            }
            if meta.path.is_ident("skip") {
                if result.skip {
                    return Err(meta.error("duplicate `skip`"));
                }
                result.skip = true;
            } else if meta.path.is_ident("blob") {
                if result.blob {
                    return Err(meta.error("duplicate `blob`"));
                }
                result.blob = true;
            } else if meta.path.is_ident("tag") {
                if result.tag {
                    return Err(meta.error("duplicate `tag`"));
                }
                result.tag = true;
            } else {
                return Err(meta.error("unknown asset field option"));
            }
            Ok(())
        })?;
    }
    *attrs = retained;
    if result.skip && (result.blob || result.tag || result.rev_present) {
        return Err(syn::Error::new(
            Span::call_site(),
            "`skip` cannot be combined with `blob`, `tag`, or `rev`",
        ));
    }
    if result.blob && result.tag {
        return Err(syn::Error::new(
            Span::call_site(),
            "`blob` and `tag` are mutually exclusive",
        ));
    }
    Ok(result)
}

fn validate_field_args(ty: &Type, args: NodeArgs) -> syn::Result<()> {
    if args.blob && type_last_ident(ty).as_deref() != Some("Blob") {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[asset(blob)]` fields must use distill_asset::Blob",
        ));
    }
    if args.tag && type_last_ident(ty).as_deref() != Some("String") {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[asset(tag)]` fields must use String",
        ));
    }
    if !args.blob && !args.skip && type_last_ident(ty).as_deref() == Some("Blob") {
        return Err(syn::Error::new_spanned(
            ty,
            "Blob fields require `#[asset(blob)]` so their schema and artifact encoding cannot disagree",
        ));
    }
    Ok(())
}

fn type_last_ident(ty: &Type) -> Option<String> {
    let Type::Path(path) = ty else { return None };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn expand_struct(
    args: TypeArgs,
    uuid: [u8; 16],
    mut item: ItemStruct,
) -> syn::Result<TokenStream2> {
    validate_item(&item.ident, &item.generics)?;
    if has_repr(&item.attrs, "packed")? {
        return Err(syn::Error::new_spanned(
            &item.ident,
            "packed asset structs are unsupported because generated safe encoders must borrow fields",
        ));
    }
    let fields = fields_info(&mut item.fields)?;
    let body = common_impls(
        &item.ident,
        &args,
        uuid,
        struct_reflect(&item.ident, &fields, args.rev.map_or(0, |r| r.0))?,
    );
    Ok(quote! { #item #body })
}

fn has_repr(attrs: &[Attribute], wanted: &str) -> syn::Result<bool> {
    let mut found = false;
    for attr in attrs {
        if attr.path().is_ident("repr") {
            attr.parse_nested_meta(|meta| {
                found |= meta.path.is_ident(wanted);
                Ok(())
            })?;
        }
    }
    Ok(found)
}

fn struct_reflect(ident: &Ident, fields: &[FieldInfo], rev: u32) -> syn::Result<TokenStream2> {
    let default_probe = default_probe(ident);
    let layout_fields = fields.iter().map(layout_field);
    let mut logical: Vec<_> = fields.iter().filter(|field| !field.args.skip).collect();
    logical.sort_by_key(|field| field.name.nfc().collect::<String>().into_bytes());
    let logical_fields = logical.iter().map(|field| {
        let name = &field.name;
        let rev = field.args.rev;
        let ty = &field.ty;
        quote! {
            builder.string(#name);
            builder.u32(#rev);
            <#ty as ::distill_asset::AssetReflect>::logical(builder);
        }
    });
    let encode_fields = logical.iter().map(|field| {
        let member = &field.member;
        quote! {
            sink.push()?;
            ::distill_asset::AssetReflect::encode(&self.#member, sink)?;
        }
    });
    let authored_fields = logical.iter().map(|field| {
        let member = &field.member;
        let name = &field.name;
        quote! {
            object.insert(#name.to_owned(), ::distill_asset::AssetReflect::to_authored(&self.#member));
        }
    });
    let defaults = logical.iter().map(default_collect_field);
    let serializable_count = logical.len() as u32;
    Ok(quote! {
        unsafe impl ::distill_asset::AssetReflect for #ident {
            fn layout(
                builder: &mut ::distill_asset::build::LayoutBuilder,
                offset: u32,
            ) -> ::distill_asset::NativeLayoutNode {
                if let Some(backref) = builder.backref::<Self>(offset) {
                    return backref;
                }
                builder.push_frame::<Self>();
                let whole_drop = builder.register_drop_if_needed::<Self>();
                let fields = vec![#(#layout_fields),*];
                let fields = builder.leak_fields(fields);
                builder.pop_frame::<Self>();
                ::distill_asset::NativeLayoutNode::Struct {
                    offset,
                    size: ::distill_asset::build::checked_size::<Self>(),
                    align: ::distill_asset::build::checked_align::<Self>(),
                    whole_drop,
                    fields,
                }
            }

            fn logical(builder: &mut ::distill_asset::build::LogicalBuilder) {
                if let Some(distance) = builder.enter::<Self>() {
                    builder.backref(distance);
                    return;
                }
                builder.byte(0x02);
                builder.u32(#rev);
                builder.u32(#serializable_count);
                #(#logical_fields)*
                builder.exit::<Self>();
            }

            fn encode(
                &self,
                sink: &mut dyn ::distill_asset::EncodeSink,
            ) -> ::core::result::Result<(), ::distill_asset::CallbackPanic> {
                sink.begin(::distill_asset::EncodeContainer::Struct, #serializable_count)?;
                #(#encode_fields)*
                sink.finish()
            }

            fn to_authored(&self) -> ::distill_asset::AuthoredValue {
                let mut object = ::std::collections::BTreeMap::new();
                #(#authored_fields)*
                ::distill_asset::AuthoredValue::Object(object)
            }

            fn default_writer() -> Option<::distill_asset::DefaultWriter> {
                #default_probe
            }

            fn collect_default_nodes(collector: &mut ::distill_asset::DefaultCollector) {
                let Some(node) = collector.begin::<Self>() else {
                    return;
                };
                #(#defaults)*
            }

        }
    })
}

fn layout_field(field: &FieldInfo) -> TokenStream2 {
    let member = &field.member;
    let name = &field.name;
    let declaration = field.declaration;
    let ty = &field.ty;
    if field.args.skip {
        quote! {
            {
                let writer = builder.register_skip::<#ty>();
                ::distill_asset::NativeField {
                    name: #name,
                    declaration_index: #declaration,
                    node: ::distill_asset::NativeLayoutNode::Skip {
                        offset: ::core::mem::offset_of!(Self, #member) as u32,
                        size: ::distill_asset::build::checked_size::<#ty>(),
                        align: ::distill_asset::build::checked_align::<#ty>(),
                        writer,
                    },
                }
            }
        }
    } else {
        quote! {
            ::distill_asset::NativeField {
                name: #name,
                declaration_index: #declaration,
                node: <#ty as ::distill_asset::AssetReflect>::layout(
                    builder,
                    ::core::mem::offset_of!(Self, #member) as u32,
                ),
            }
        }
    }
}

fn default_collect_field(field: &&FieldInfo) -> TokenStream2 {
    let name = &field.name;
    let ty = &field.ty;
    quote! {
        if let Some(writer) = <#ty as ::distill_asset::AssetReflect>::default_writer() {
            collector.add(node, vec![::distill_asset::PathStep::Field(#name)], writer);
        }
        <#ty as ::distill_asset::AssetReflect>::collect_default_nodes(collector);
    }
}

fn common_impls(
    ident: &Ident,
    args: &TypeArgs,
    uuid: [u8; 16],
    reflect_impl: TokenStream2,
) -> TokenStream2 {
    let uuid = uuid.iter();
    let build_only = args.build_only;
    quote! {
        #reflect_impl

        unsafe impl ::distill_asset::AssetType for #ident {
            const TYPE_UUID: ::distill_asset::TypeUuid =
                ::distill_asset::TypeUuid([#(#uuid),*]);

            fn descriptor() -> &'static ::distill_asset::AssetRuntimeDescriptor {
                static DESCRIPTOR: ::std::sync::OnceLock<::distill_asset::AssetRuntimeDescriptor> =
                    ::std::sync::OnceLock::new();
                DESCRIPTOR.get_or_init(|| {
                    ::distill_asset::build::build_descriptor::<#ident>(
                        ::distill_asset::build::AssetMetadata {
                            type_uuid: <#ident as ::distill_asset::AssetType>::TYPE_UUID,
                            build_only: #build_only,
                        },
                    )
                })
            }
        }

        impl ::distill_asset::AssetDefaults for #ident {
            fn default_table() -> &'static ::distill_asset::DefaultTable<Self> {
                static TABLE: ::std::sync::OnceLock<::distill_asset::DefaultTable<#ident>> =
                    ::std::sync::OnceLock::new();
                TABLE.get_or_init(|| {
                    let mut collector = ::distill_asset::DefaultCollector::default();
                    <#ident as ::distill_asset::AssetReflect>::collect_default_nodes(&mut collector);
                    ::distill_asset::defaults::make_table::<#ident>(
                        <#ident as ::distill_asset::AssetReflect>::default_writer(),
                        collector.finish(),
                    )
                })
            }
        }
    }
}

struct VariantInfo {
    ident: Ident,
    args: NodeArgs,
    fields: Vec<FieldInfo>,
    style: VariantStyle,
    discriminant: Option<Expr>,
    declaration: u32,
}

#[derive(Clone, Copy)]
enum VariantStyle {
    Unit,
    Named,
    Unnamed,
}

fn expand_enum(args: TypeArgs, uuid: [u8; 16], mut item: ItemEnum) -> syn::Result<TokenStream2> {
    validate_item(&item.ident, &item.generics)?;
    if item.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &item,
            "uninhabited asset enums are unsupported",
        ));
    }
    let repr = integer_repr(&item.attrs)?;
    if item.variants.len() > 1 && repr.is_none() {
        return Err(syn::Error::new_spanned(
            &item.ident,
            "multi-variant asset enums require an explicit integer `#[repr(u8/u16/u32/u64/u128/i8/i16/i32/i64/i128)]` so native tag geometry is measurable",
        ));
    }
    let mut variants = Vec::new();
    for (index, variant) in item.variants.iter_mut().enumerate() {
        let node_args = take_node_args(&mut variant.attrs, true)?;
        let style = match variant.fields {
            Fields::Unit => VariantStyle::Unit,
            Fields::Named(_) => VariantStyle::Named,
            Fields::Unnamed(_) => VariantStyle::Unnamed,
        };
        let fields = fields_info(&mut variant.fields)?;
        variants.push(VariantInfo {
            ident: variant.ident.clone(),
            args: node_args,
            fields,
            style,
            discriminant: variant
                .discriminant
                .as_ref()
                .map(|(_, value)| value.clone()),
            declaration: index as u32,
        });
    }
    let reflect = enum_reflect(&item.ident, &variants, args.rev.map_or(0, |r| r.0), repr)?;
    let impls = common_impls(&item.ident, &args, uuid, reflect);
    Ok(quote! { #item #impls })
}

fn integer_repr(attrs: &[Attribute]) -> syn::Result<Option<Ident>> {
    let mut found = None;
    for attr in attrs {
        if !attr.path().is_ident("repr") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let Some(ident) = meta.path.get_ident() else {
                return Ok(());
            };
            if matches!(
                ident.to_string().as_str(),
                "u8" | "u16" | "u32" | "u64" | "u128" | "i8" | "i16" | "i32" | "i64" | "i128"
            ) {
                if found.is_some() {
                    return Err(meta.error("asset enum has multiple integer reprs"));
                }
                found = Some(ident.clone());
            }
            Ok(())
        })?;
    }
    Ok(found)
}

fn enum_reflect(
    ident: &Ident,
    variants: &[VariantInfo],
    rev: u32,
    repr: Option<Ident>,
) -> syn::Result<TokenStream2> {
    let default_probe = default_probe(ident);
    if variants.len() == 1 && repr.is_none() && !variants[0].fields.is_empty() {
        return Err(syn::Error::new_spanned(
            ident,
            "field-bearing single-variant asset enums require an explicit integer repr",
        ));
    }
    let repr = repr.unwrap_or_else(|| Ident::new("u8", Span::call_site()));
    let tag_size = repr_size(&repr);
    let tag_variants = variants.iter().map(|variant| {
        let ident = &variant.ident;
        match &variant.discriminant {
            Some(expr) => quote!(#ident = #expr),
            None => quote!(#ident),
        }
    });
    let layout_variants = variants
        .iter()
        .map(|variant| enum_layout_variant(variant, &repr, variants.len() == 1));

    let mut logical: Vec<_> = variants.iter().collect();
    logical.sort_by_key(|variant| {
        variant
            .ident
            .to_string()
            .nfc()
            .collect::<String>()
            .into_bytes()
    });
    let logical_variants = logical.iter().map(|variant| {
        let name = variant.ident.to_string();
        let variant_rev = variant.args.rev;
        let mut fields: Vec<_> = variant
            .fields
            .iter()
            .filter(|field| !field.args.skip)
            .collect();
        fields.sort_by_key(|field| field.name.nfc().collect::<String>().into_bytes());
        let field_count = fields.len() as u32;
        let fields = fields.iter().map(|field| {
            let name = &field.name;
            let rev = field.args.rev;
            let ty = &field.ty;
            quote! {
                builder.string(#name);
                builder.u32(#rev);
                <#ty as ::distill_asset::AssetReflect>::logical(builder);
            }
        });
        quote! {
            builder.string(#name);
            builder.u32(#variant_rev);
            builder.byte(0x02);
            builder.u32(0);
            builder.u32(#field_count);
            #(#fields)*
        }
    });
    let encode_arms = logical
        .iter()
        .enumerate()
        .map(|(wire_index, variant)| enum_encode_arm(variant, wire_index as u32));
    let authored_arms = variants.iter().map(enum_authored_arm);
    let default_collect = variants.iter().map(enum_default_collect);
    let variant_count = variants.len() as u32;
    let tag_encoding = if variants.len() == 1 {
        quote!(::distill_asset::NativeTagEncoding::Single)
    } else {
        quote!(::distill_asset::NativeTagEncoding::Direct { offset: 0, size: #tag_size })
    };
    Ok(quote! {
        unsafe impl ::distill_asset::AssetReflect for #ident {
            fn layout(
                builder: &mut ::distill_asset::build::LayoutBuilder,
                offset: u32,
            ) -> ::distill_asset::NativeLayoutNode {
                if let Some(backref) = builder.backref::<Self>(offset) {
                    return backref;
                }
                builder.push_frame::<Self>();
                #[repr(#repr)]
                enum __DistillAssetTag { #(#tag_variants),* }
                let whole_drop = builder.register_drop_if_needed::<Self>();
                let variants = vec![#(#layout_variants),*];
                let variants = builder.leak_variants(variants);
                builder.pop_frame::<Self>();
                ::distill_asset::NativeLayoutNode::Enum {
                    offset,
                    size: ::distill_asset::build::checked_size::<Self>(),
                    align: ::distill_asset::build::checked_align::<Self>(),
                    tag: #tag_encoding,
                    whole_drop,
                    variants,
                }
            }

            fn logical(builder: &mut ::distill_asset::build::LogicalBuilder) {
                if let Some(distance) = builder.enter::<Self>() {
                    builder.backref(distance);
                    return;
                }
                builder.byte(0x03);
                builder.u32(#rev);
                builder.u32(#variant_count);
                #(#logical_variants)*
                builder.exit::<Self>();
            }

            fn encode(
                &self,
                sink: &mut dyn ::distill_asset::EncodeSink,
            ) -> ::core::result::Result<(), ::distill_asset::CallbackPanic> {
                match self { #(#encode_arms),* }
            }

            fn to_authored(&self) -> ::distill_asset::AuthoredValue {
                match self { #(#authored_arms),* }
            }

            fn default_writer() -> Option<::distill_asset::DefaultWriter> {
                #default_probe
            }

            fn collect_default_nodes(collector: &mut ::distill_asset::DefaultCollector) {
                let Some(node) = collector.begin::<Self>() else {
                    return;
                };
                #(#default_collect)*
            }

        }
    })
}

fn enum_layout_variant(variant: &VariantInfo, repr: &Ident, single: bool) -> TokenStream2 {
    let variant_ident = &variant.ident;
    let declaration = variant.declaration;
    let name = variant.ident.to_string();
    let field_layouts = variant.fields.iter().map(|field| {
        let ty = &field.ty;
        let field_name = &field.name;
        let field_declaration = field.declaration;
        if field.args.skip {
            quote! {
                {
                    let field_align = ::distill_asset::build::checked_align::<#ty>();
                    cursor = ::distill_asset::build::align_up(cursor, field_align);
                    let field_offset = cursor;
                    cursor = ::distill_asset::build::checked_add(
                        cursor,
                        ::distill_asset::build::checked_size::<#ty>(),
                    );
                    payload_align = payload_align.max(field_align);
                    let writer = builder.register_skip::<#ty>();
                    fields.push(::distill_asset::NativeField {
                        name: #field_name,
                        declaration_index: #field_declaration,
                        node: ::distill_asset::NativeLayoutNode::Skip {
                            offset: field_offset,
                            size: ::distill_asset::build::checked_size::<#ty>(),
                            align: field_align,
                            writer,
                        },
                    });
                }
            }
        } else {
            quote! {
                {
                    let field_align = ::distill_asset::build::checked_align::<#ty>();
                    cursor = ::distill_asset::build::align_up(cursor, field_align);
                    let field_offset = cursor;
                    cursor = ::distill_asset::build::checked_add(
                        cursor,
                        ::distill_asset::build::checked_size::<#ty>(),
                    );
                    payload_align = payload_align.max(field_align);
                    fields.push(::distill_asset::NativeField {
                        name: #field_name,
                        declaration_index: #field_declaration,
                        node: <#ty as ::distill_asset::AssetReflect>::layout(builder, field_offset),
                    });
                }
            }
        }
    });
    let tag = if single {
        quote!(::distill_asset::NativeVariantTag::Single)
    } else {
        let mask = repr_mask(repr);
        quote! {
            ::distill_asset::NativeVariantTag::Direct {
                value: ((__DistillAssetTag::#variant_ident as #repr) as u128) & #mask,
            }
        }
    };
    let repr_size = repr_size(repr) as u32;
    quote! {
        {
            // A multi-variant primitive-repr enum lays each variant out as a
            // C-like frame beginning with the tag. Field offsets are enum-
            // relative, so retain the tag prefix in this variant node rather
            // than fabricating a separately shifted payload frame.
            let mut cursor = if #single { 0u32 } else { #repr_size };
            let mut payload_align = 1u32;
            let mut fields = Vec::new();
            #(#field_layouts)*
            let payload_size = ::distill_asset::build::align_up(cursor, payload_align);
            let fields = builder.leak_fields(fields);
            let node = builder.leak_node(::distill_asset::NativeLayoutNode::Struct {
                offset: 0,
                size: payload_size,
                align: payload_align,
                whole_drop: None,
                fields,
            });
            ::distill_asset::NativeVariant {
                name: #name,
                declaration_index: #declaration,
                node,
                tag: #tag,
            }
        }
    }
}

fn enum_pattern(variant: &VariantInfo) -> (TokenStream2, Vec<&Ident>) {
    let variant_ident = &variant.ident;
    let bindings: Vec<_> = variant.fields.iter().map(|field| &field.binding).collect();
    let pattern = match variant.style {
        VariantStyle::Unit => quote!(Self::#variant_ident),
        VariantStyle::Named => quote!(Self::#variant_ident { #(#bindings),* }),
        VariantStyle::Unnamed => quote!(Self::#variant_ident(#(#bindings),*)),
    };
    (pattern, bindings)
}

fn enum_encode_arm(variant: &&VariantInfo, wire_index: u32) -> TokenStream2 {
    let (pattern, _) = enum_pattern(variant);
    let fields: Vec<_> = variant
        .fields
        .iter()
        .filter(|field| !field.args.skip)
        .collect();
    let len = fields.len() as u32;
    let encodes = fields.iter().map(|field| {
        let binding = &field.binding;
        quote! {
            sink.push()?;
            ::distill_asset::AssetReflect::encode(#binding, sink)?;
        }
    });
    quote! {
        #pattern => {
            sink.begin(::distill_asset::EncodeContainer::Variant(#wire_index), #len)?;
            #(#encodes)*
            sink.finish()
        }
    }
}

fn enum_authored_arm(variant: &VariantInfo) -> TokenStream2 {
    let (pattern, _) = enum_pattern(variant);
    let name = variant.ident.to_string();
    let fields = variant.fields.iter().filter(|field| !field.args.skip).map(|field| {
        let field_name = &field.name;
        let binding = &field.binding;
        quote! {
            __distill_authored_payload.insert(#field_name.to_owned(), ::distill_asset::AssetReflect::to_authored(#binding));
        }
    });
    quote! {
        #pattern => {
            let mut __distill_authored_payload = ::std::collections::BTreeMap::new();
            #(#fields)*
            let mut value = ::std::collections::BTreeMap::new();
            value.insert(#name.to_owned(), ::distill_asset::AuthoredValue::Object(__distill_authored_payload));
            ::distill_asset::AuthoredValue::Object(value)
        }
    }
}

fn enum_default_collect(variant: &VariantInfo) -> TokenStream2 {
    let variant_name = variant.ident.to_string();
    let fields = variant
        .fields
        .iter()
        .filter(|field| !field.args.skip)
        .map(|field| {
            let field_name = &field.name;
            let ty = &field.ty;
            quote! {
                if let Some(writer) = <#ty as ::distill_asset::AssetReflect>::default_writer() {
                    collector.add(
                        node,
                        vec![
                            ::distill_asset::PathStep::Variant(#variant_name),
                            ::distill_asset::PathStep::Field(#field_name),
                        ],
                        writer,
                    );
                }
                <#ty as ::distill_asset::AssetReflect>::collect_default_nodes(collector);
            }
        });
    quote! {
        #(#fields)*
    }
}

fn repr_size(repr: &Ident) -> u8 {
    match repr.to_string().as_str() {
        "u8" | "i8" => 1,
        "u16" | "i16" => 2,
        "u32" | "i32" => 4,
        "u64" | "i64" => 8,
        "u128" | "i128" => 16,
        _ => unreachable!(),
    }
}

fn default_probe(ident: &Ident) -> TokenStream2 {
    quote! {
        {
            struct __DistillDefaultProbe<T>(::core::marker::PhantomData<fn() -> T>);
            trait __DistillGetDefault {
                fn get(self) -> Option<::distill_asset::DefaultWriter>;
            }
            impl<T> __DistillGetDefault for &&__DistillDefaultProbe<T> {
                fn get(self) -> Option<::distill_asset::DefaultWriter> {
                    None
                }
            }
            impl<T> __DistillGetDefault for &__DistillDefaultProbe<T>
            where
                T: ::core::default::Default + ::distill_asset::AssetReflect,
            {
                fn get(self) -> Option<::distill_asset::DefaultWriter> {
                    Some(::distill_asset::defaults::write_default::<T>)
                }
            }
            let probe = __DistillDefaultProbe::<#ident>(::core::marker::PhantomData);
            (&probe).get()
        }
    }
}

fn repr_mask(repr: &Ident) -> u128 {
    let bits = u32::from(repr_size(repr)) * 8;
    if bits == 128 {
        u128::MAX
    } else {
        (1u128 << bits) - 1
    }
}

fn parse_uuid(value: &str) -> Option<[u8; 16]> {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return None;
    }
    let mut out = [0u8; 16];
    let mut source = 0usize;
    let mut destination = 0usize;
    while source < bytes.len() {
        if matches!(source, 8 | 13 | 18 | 23) {
            if bytes[source] != b'-' {
                return None;
            }
            source += 1;
            continue;
        }
        let high = hex(bytes[source])?;
        let low = hex(bytes[source + 1])?;
        out[destination] = high << 4 | low;
        source += 2;
        destination += 1;
    }
    Some(out)
}

fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}
