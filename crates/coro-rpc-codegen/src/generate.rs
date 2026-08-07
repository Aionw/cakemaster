use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use heck::{ToShoutySnakeCase, ToSnakeCase, ToUpperCamelCase};
use md5::{Digest, Md5};
use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};

use crate::CodegenError;
use crate::ast::{
    Document, Enum, EnumRepr, Field, Function, Service, Struct, Type, Typedef, Union,
};

const TYPE_INT32: u8 = 1;
const TYPE_UINT32: u8 = 2;
const TYPE_INT64: u8 = 3;
const TYPE_UINT64: u8 = 4;
const TYPE_INT8: u8 = 5;
const TYPE_UINT8: u8 = 6;
const TYPE_INT16: u8 = 7;
const TYPE_UINT16: u8 = 8;
const TYPE_BOOL: u8 = 11;
const TYPE_CHAR8: u8 = 12;
const TYPE_FLOAT32: u8 = 17;
const TYPE_FLOAT64: u8 = 18;
const TYPE_STRING: u8 = 128;
const TYPE_MAP: u8 = 130;
const TYPE_SET: u8 = 131;
const TYPE_CONTAINER: u8 = 132;
const TYPE_OPTIONAL: u8 = 133;
const TYPE_VARIANT: u8 = 134;
const TYPE_EXPECTED: u8 = 135;
const TYPE_MONOSTATE: u8 = 250;
const TYPE_STRUCT: u8 = 253;
const TYPE_END: u8 = 255;

struct TypeSchemas<'a> {
    typedefs: BTreeMap<&'a str, &'a Typedef>,
    structs: BTreeMap<&'a str, &'a Struct>,
    unions: BTreeMap<&'a str, &'a Union>,
    enums: BTreeMap<&'a str, &'a Enum>,
}

impl<'a> TypeSchemas<'a> {
    fn new(document: &'a Document) -> Self {
        Self {
            typedefs: document
                .typedefs
                .iter()
                .map(|item| (item.name.as_str(), item))
                .collect(),
            structs: document
                .structs
                .iter()
                .map(|item| (item.name.as_str(), item))
                .collect(),
            unions: document
                .unions
                .iter()
                .map(|item| (item.name.as_str(), item))
                .collect(),
            enums: document
                .enums
                .iter()
                .map(|item| (item.name.as_str(), item))
                .collect(),
        }
    }
}

pub(crate) fn generate(
    document: &Document,
    runtime: &syn::Path,
    path: &Path,
) -> Result<TokenStream, CodegenError> {
    validate(document, path)?;
    let typedefs = document
        .typedefs
        .iter()
        .map(|typedef| generate_typedef(typedef, runtime, path))
        .collect::<Result<Vec<_>, _>>()?;
    let enums = document
        .enums
        .iter()
        .map(|enumeration| generate_enum(enumeration, runtime, path))
        .collect::<Result<Vec<_>, _>>()?;
    let unions = document
        .unions
        .iter()
        .map(|union| generate_union(union, runtime, path))
        .collect::<Result<Vec<_>, _>>()?;
    let structs = document
        .structs
        .iter()
        .map(|structure| generate_struct(structure, runtime, path))
        .collect::<Result<Vec<_>, _>>()?;
    let services = document
        .services
        .iter()
        .map(|service| generate_service(document, service, runtime, path))
        .collect::<Result<Vec<_>, _>>()?;

    let mut output = quote! {
        #(#typedefs)*
        #(#enums)*
        #(#unions)*
        #(#structs)*
        #(#services)*
    };
    for component in document.rust_namespace.iter().rev() {
        let module = rust_ident(&component.to_snake_case(), "namespace", path)?;
        output = quote! {
            pub mod #module {
                #output
            }
        };
    }
    Ok(output)
}

fn validate(document: &Document, path: &Path) -> Result<(), CodegenError> {
    let mut rust_types = BTreeSet::new();
    let mut wire_routes = BTreeMap::<u32, String>::new();
    let mut wire_names = BTreeSet::new();

    for typedef in &document.typedefs {
        register_rust_type(&mut rust_types, &typedef.name, path)?;
        ensure_non_void(&typedef.target, "typedef", path)?;
    }
    for enumeration in &document.enums {
        register_rust_type(&mut rust_types, &enumeration.name, path)?;
        validate_enum(enumeration, path)?;
    }
    for union in &document.unions {
        register_rust_type(&mut rust_types, &union.name, path)?;
        validate_union(union, path)?;
    }
    for structure in &document.structs {
        register_rust_type(&mut rust_types, &structure.name, path)?;
        if structure.fields.is_empty() {
            return Err(invalid(
                path,
                format!("struct {} must contain at least one field", structure.name),
            ));
        }
        validate_fields(
            &structure.fields,
            &format!("struct {}", structure.name),
            path,
        )?;
        if structure.cpp_u64_pair {
            validate_cpp_u64_pair(structure, path)?;
        }
    }

    let known_types = document
        .typedefs
        .iter()
        .map(|item| item.name.as_str())
        .chain(document.enums.iter().map(|item| item.name.as_str()))
        .chain(document.unions.iter().map(|item| item.name.as_str()))
        .chain(document.structs.iter().map(|item| item.name.as_str()))
        .collect::<BTreeSet<_>>();
    let schemas = TypeSchemas::new(document);
    for typedef in &document.typedefs {
        validate_type_references(&typedef.target, &known_types, path)?;
        validate_ordered_containers(&typedef.target, &schemas.typedefs, &schemas.enums, path)?;
    }
    for structure in &document.structs {
        for field in &structure.fields {
            validate_type_references(&field.type_, &known_types, path)?;
            validate_ordered_containers(&field.type_, &schemas.typedefs, &schemas.enums, path)?;
        }
    }
    for union in &document.unions {
        for alternative in &union.alternatives {
            validate_type_references(&alternative.type_, &known_types, path)?;
            validate_ordered_containers(
                &alternative.type_,
                &schemas.typedefs,
                &schemas.enums,
                path,
            )?;
        }
    }

    let mut generated_items = rust_types;
    let mut rust_services = BTreeSet::new();
    for service in &document.services {
        let service_name = service.name.to_upper_camel_case();
        if !rust_services.insert(service_name.clone()) {
            return Err(invalid(
                path,
                format!("duplicate generated service name {service_name}"),
            ));
        }
        for item in [
            service_name.clone(),
            format!("{service_name}Client"),
            format!("{service_name}Server"),
        ] {
            rust_ident(&item, "generated service item", path)?;
            if !generated_items.insert(item.clone()) {
                return Err(invalid(
                    path,
                    format!("duplicate generated Rust item {item}"),
                ));
            }
        }
        let mut methods = BTreeSet::new();
        for function in &service.functions {
            let method = function.name.to_snake_case();
            if matches!(method.as_str(), "new" | "connect" | "inner" | "into_inner") {
                return Err(invalid(
                    path,
                    format!(
                        "service method {} conflicts with generated client API {method}",
                        function.name
                    ),
                ));
            }
            if !methods.insert(method.clone()) {
                return Err(invalid(
                    path,
                    format!(
                        "service {} has duplicate generated method name {method}",
                        service.name
                    ),
                ));
            }
            rust_ident(&method, "method", path)?;
            validate_fields(
                &function.parameters,
                &format!("method {}::{}", service.name, function.name),
                path,
            )?;
            validate_type_references(&function.returns, &known_types, path)?;
            validate_ordered_containers(
                &function.returns,
                &schemas.typedefs,
                &schemas.enums,
                path,
            )?;
            for parameter in &function.parameters {
                validate_type_references(&parameter.type_, &known_types, path)?;
                validate_ordered_containers(
                    &parameter.type_,
                    &schemas.typedefs,
                    &schemas.enums,
                    path,
                )?;
            }
            if function.attachment
                && function.parameters.iter().any(|parameter| {
                    matches!(
                        parameter.name.to_snake_case().as_str(),
                        "rpc_attachment" | "rpc_context"
                    )
                })
            {
                return Err(invalid(
                    path,
                    format!(
                        "attachment method {}::{} reserves parameter names rpc_attachment and rpc_context",
                        service.name, function.name
                    ),
                ));
            }

            let wire_name = wire_name(document, service, function);
            if !wire_names.insert(wire_name.clone()) {
                return Err(invalid(
                    path,
                    format!("duplicate wire method name {wire_name}"),
                ));
            }
            let route = md5_hash32(wire_name.as_bytes());
            if let Some(existing) = wire_routes.insert(route, wire_name.clone()) {
                return Err(invalid(
                    path,
                    format!("route ID collision {route:#010x} between {existing} and {wire_name}"),
                ));
            }
        }
    }

    for typedef in &document.typedefs {
        type_literal(
            &typedef.target,
            false,
            &schemas,
            &mut vec![typedef.name.clone()],
            path,
        )?;
    }
    for structure in &document.structs {
        for field in ordered_fields(&structure.fields) {
            type_literal(
                &field.type_,
                field.optional,
                &schemas,
                &mut vec![structure.name.clone()],
                path,
            )?;
        }
    }
    for union in &document.unions {
        type_literal(
            &Type::Named(union.name.clone()),
            false,
            &schemas,
            &mut Vec::new(),
            path,
        )?;
    }
    for service in &document.services {
        for function in &service.functions {
            let mut stack = Vec::new();
            request_type_literal(&function.parameters, &schemas, &mut stack, path)?;
            type_literal(&function.returns, false, &schemas, &mut stack, path)?;
        }
    }
    Ok(())
}

fn validate_fields(fields: &[Field], owner: &str, path: &Path) -> Result<(), CodegenError> {
    let mut ids = BTreeSet::new();
    let mut names = BTreeSet::new();
    for field in fields {
        if !ids.insert(field.id) {
            return Err(invalid(
                path,
                format!("{owner} has duplicate field ID {}", field.id),
            ));
        }
        let name = field.name.to_snake_case();
        if !names.insert(name.clone()) {
            return Err(invalid(
                path,
                format!("{owner} has duplicate generated field name {name}"),
            ));
        }
        rust_ident(&name, "field", path)?;
        ensure_non_void(&field.type_, owner, path)?;
    }
    Ok(())
}

fn validate_enum(enumeration: &Enum, path: &Path) -> Result<(), CodegenError> {
    if enumeration.variants.is_empty() {
        return Err(invalid(
            path,
            format!(
                "enum {} must contain at least one variant",
                enumeration.name
            ),
        ));
    }

    let mut names = BTreeSet::new();
    let mut values = BTreeMap::new();
    for variant in &enumeration.variants {
        if enumeration.repr == EnumRepr::U8 && u8::try_from(variant.value).is_err() {
            return Err(invalid(
                path,
                format!(
                    "enum {} value {} does not fit coro_rpc.repr u8",
                    enumeration.name, variant.value
                ),
            ));
        }
        let name = variant.name.to_upper_camel_case();
        rust_ident(&name, "enum variant", path)?;
        if !names.insert(name.clone()) {
            return Err(invalid(
                path,
                format!(
                    "enum {} has duplicate generated variant name {name}",
                    enumeration.name
                ),
            ));
        }
        if let Some(existing) = values.insert(variant.value, variant.name.as_str()) {
            return Err(invalid(
                path,
                format!(
                    "enum {} variants {existing} and {} both use value {}",
                    enumeration.name, variant.name, variant.value
                ),
            ));
        }
    }
    Ok(())
}

fn validate_union(union: &Union, path: &Path) -> Result<(), CodegenError> {
    if union.alternatives.is_empty() {
        return Err(invalid(
            path,
            format!("union {} must contain at least one alternative", union.name),
        ));
    }

    validate_fields(&union.alternatives, &format!("union {}", union.name), path)?;
    if union.expected {
        expected_alternatives(union, path)?;
        return Ok(());
    }
    if union.alternatives.len() >= 256 {
        return Err(invalid(
            path,
            format!(
                "union {} must contain fewer than 256 alternatives",
                union.name
            ),
        ));
    }

    let mut names = BTreeSet::new();
    for (index, alternative) in ordered_fields(&union.alternatives).into_iter().enumerate() {
        let expected_id = i64::try_from(index + 1).expect("union alternatives are limited to 255");
        if alternative.id != expected_id {
            return Err(invalid(
                path,
                format!(
                    "union {} field IDs must be contiguous from 1; expected {expected_id}, got {}",
                    union.name, alternative.id
                ),
            ));
        }
        let name = alternative.name.to_upper_camel_case();
        rust_ident(&name, "union variant", path)?;
        if !names.insert(name.clone()) {
            return Err(invalid(
                path,
                format!(
                    "union {} has duplicate generated variant name {name}",
                    union.name
                ),
            ));
        }
    }
    Ok(())
}

fn validate_cpp_u64_pair(structure: &Struct, path: &Path) -> Result<(), CodegenError> {
    let fields = ordered_fields(&structure.fields);
    let valid = matches!(
        fields.as_slice(),
        [first, second]
            if first.id == 1
                && second.id == 2
                && !first.optional
                && !second.optional
                && first.type_ == Type::U64
                && second.type_ == Type::U64
    );
    if valid {
        Ok(())
    } else {
        Err(invalid(
            path,
            format!(
                "struct {} with coro_rpc.cpp_u64_pair must contain exactly two required u64 fields numbered 1 and 2",
                structure.name
            ),
        ))
    }
}

fn expected_alternatives<'a>(
    union: &'a Union,
    path: &Path,
) -> Result<(Option<&'a Field>, &'a Field), CodegenError> {
    let alternatives = ordered_fields(&union.alternatives);
    match alternatives.as_slice() {
        [error] if error.id == 1 && error.name == "error" => Ok((None, error)),
        [value, error]
            if value.id == 1 && value.name == "value" && error.id == 2 && error.name == "error" =>
        {
            Ok((Some(value), error))
        }
        _ => Err(invalid(
            path,
            format!(
                "expected union {} must contain `1: <type> value, 2: <type> error`, or only `1: <type> error` for expected<void, E>",
                union.name
            ),
        )),
    }
}

fn register_rust_type(
    names: &mut BTreeSet<String>,
    name: &str,
    path: &Path,
) -> Result<(), CodegenError> {
    let name = name.to_upper_camel_case();
    rust_ident(&name, "type", path)?;
    if !names.insert(name.clone()) {
        return Err(invalid(
            path,
            format!("duplicate generated type name {name}"),
        ));
    }
    Ok(())
}

fn validate_type_references(
    type_: &Type,
    known_types: &BTreeSet<&str>,
    path: &Path,
) -> Result<(), CodegenError> {
    match type_ {
        Type::Named(name) if !known_types.contains(name.as_str()) => Err(invalid(
            path,
            format!("unknown Thrift type {name}; includes are not supported yet"),
        )),
        Type::List(value) | Type::Set(value) => validate_type_references(value, known_types, path),
        Type::Map(key, value) => {
            validate_type_references(key, known_types, path)?;
            validate_type_references(value, known_types, path)
        }
        _ => Ok(()),
    }
}

fn validate_ordered_containers(
    type_: &Type,
    typedefs: &BTreeMap<&str, &Typedef>,
    enums: &BTreeMap<&str, &Enum>,
    path: &Path,
) -> Result<(), CodegenError> {
    match type_ {
        Type::List(value) => validate_ordered_containers(value, typedefs, enums, path),
        Type::Set(value) => {
            if !is_rust_ord(value, typedefs, enums, &mut Vec::new()) {
                return Err(invalid(
                    path,
                    "Thrift set element type must implement Rust Ord; floating-point and generated struct/union elements are not supported",
                ));
            }
            validate_ordered_containers(value, typedefs, enums, path)
        }
        Type::Map(key, value) => {
            if !is_rust_ord(key, typedefs, enums, &mut Vec::new()) {
                return Err(invalid(
                    path,
                    "Thrift map key type must implement Rust Ord; floating-point and generated struct/union keys are not supported",
                ));
            }
            validate_ordered_containers(key, typedefs, enums, path)?;
            validate_ordered_containers(value, typedefs, enums, path)
        }
        Type::Named(_) => Ok(()),
        _ => Ok(()),
    }
}

fn is_rust_ord(
    type_: &Type,
    typedefs: &BTreeMap<&str, &Typedef>,
    enums: &BTreeMap<&str, &Enum>,
    stack: &mut Vec<String>,
) -> bool {
    match type_ {
        Type::Bool
        | Type::I8
        | Type::U8
        | Type::I16
        | Type::U16
        | Type::I32
        | Type::U32
        | Type::I64
        | Type::U64
        | Type::String
        | Type::Binary => true,
        Type::List(value) | Type::Set(value) => is_rust_ord(value, typedefs, enums, stack),
        Type::Map(key, value) => {
            is_rust_ord(key, typedefs, enums, stack) && is_rust_ord(value, typedefs, enums, stack)
        }
        Type::Named(name) => {
            if enums.contains_key(name.as_str()) {
                return true;
            }
            if stack.iter().any(|item| item == name) {
                return false;
            }
            let Some(typedef) = typedefs.get(name.as_str()) else {
                return false;
            };
            stack.push(name.clone());
            let ordered = is_rust_ord(&typedef.target, typedefs, enums, stack);
            stack.pop();
            ordered
        }
        Type::Void | Type::F32 | Type::F64 => false,
    }
}

fn ensure_non_void(type_: &Type, owner: &str, path: &Path) -> Result<(), CodegenError> {
    match type_ {
        Type::Void => Err(invalid(path, format!("void is not valid inside {owner}"))),
        Type::List(value) | Type::Set(value) => ensure_non_void(value, owner, path),
        Type::Map(key, value) => {
            ensure_non_void(key, owner, path)?;
            ensure_non_void(value, owner, path)
        }
        _ => Ok(()),
    }
}

fn generate_typedef(
    typedef: &Typedef,
    runtime: &syn::Path,
    path: &Path,
) -> Result<TokenStream, CodegenError> {
    let name = rust_ident(&typedef.name.to_upper_camel_case(), "typedef", path)?;
    let target = rust_type(&typedef.target, false, runtime, path)?;
    Ok(quote! {
        pub type #name = #target;
    })
}

fn generate_enum(
    enumeration: &Enum,
    runtime: &syn::Path,
    path: &Path,
) -> Result<TokenStream, CodegenError> {
    let name = rust_ident(&enumeration.name.to_upper_camel_case(), "enum", path)?;
    let variants = enumeration
        .variants
        .iter()
        .map(|variant| rust_ident(&variant.name.to_upper_camel_case(), "enum variant", path))
        .collect::<Result<Vec<_>, _>>()?;
    let values = enumeration
        .variants
        .iter()
        .map(|variant| match enumeration.repr {
            EnumRepr::I32 => {
                let value = variant.value;
                quote! { #value }
            }
            EnumRepr::U8 => {
                let value = u8::try_from(variant.value)
                    .expect("u8 enum values are validated before generation");
                quote! { #value }
            }
        })
        .collect::<Vec<_>>();
    let repr = match enumeration.repr {
        EnumRepr::I32 => quote! { i32 },
        EnumRepr::U8 => quote! { u8 },
    };
    let invalid_value_field = match enumeration.repr {
        EnumRepr::I32 => quote! { value },
        EnumRepr::U8 => quote! { value: i32::from(value) },
    };

    Ok(quote! {
        #[repr(#repr)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum #name {
            #(#variants = #values,)*
        }

        impl ::core::convert::From<#name> for #repr {
            fn from(value: #name) -> Self {
                value as #repr
            }
        }

        impl ::core::convert::TryFrom<#repr> for #name {
            type Error = #repr;

            fn try_from(value: #repr) -> ::core::result::Result<Self, Self::Error> {
                match value {
                    #(#values => Ok(Self::#variants),)*
                    value => Err(value),
                }
            }
        }

        impl #runtime::StructPack for #name {
            fn append_type_literal(output: &mut ::std::vec::Vec<u8>) {
                <#repr as #runtime::StructPack>::append_type_literal(output);
            }

            fn encode_payload(
                &self,
                encoder: &mut #runtime::struct_pack::Encoder<'_>,
            ) -> ::core::result::Result<(), #runtime::StructPackError> {
                let value = <#repr>::from(*self);
                <#repr as #runtime::StructPack>::encode_payload(&value, encoder)
            }

            fn decode_payload(
                decoder: &mut #runtime::struct_pack::Decoder<'_>,
            ) -> ::core::result::Result<Self, #runtime::StructPackError> {
                let value = <#repr as #runtime::StructPack>::decode_payload(decoder)?;
                Self::try_from(value).map_err(|value| {
                    #runtime::StructPackError::InvalidEnumDiscriminant {
                        name: stringify!(#name),
                        #invalid_value_field,
                    }
                })
            }
        }
    })
}

fn generate_union(
    union: &Union,
    runtime: &syn::Path,
    path: &Path,
) -> Result<TokenStream, CodegenError> {
    let name = rust_ident(&union.name.to_upper_camel_case(), "union", path)?;
    if union.expected {
        let (value, error) = expected_alternatives(union, path)?;
        let value = value
            .map(|value| rust_type(&value.type_, false, runtime, path))
            .transpose()?
            .unwrap_or_else(|| quote! { () });
        let error = rust_type(&error.type_, false, runtime, path)?;
        return Ok(quote! {
            pub type #name = ::core::result::Result<#value, #error>;
        });
    }
    let alternatives = ordered_fields(&union.alternatives);
    let variant_names = alternatives
        .iter()
        .map(|alternative| {
            rust_ident(
                &alternative.name.to_upper_camel_case(),
                "union variant",
                path,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let variant_types = alternatives
        .iter()
        .map(|alternative| rust_type(&alternative.type_, false, runtime, path))
        .collect::<Result<Vec<_>, _>>()?;
    let variant_indices = (0..alternatives.len())
        .map(|index| {
            u8::try_from(index).expect("unions are validated to have fewer than 256 fields")
        })
        .collect::<Vec<_>>();
    let alternative_count = alternatives.len();

    Ok(quote! {
        #[derive(Debug, Clone, PartialEq)]
        pub enum #name {
            #(#variant_names(#variant_types),)*
        }

        impl #runtime::StructPack for #name {
            fn append_type_literal(output: &mut ::std::vec::Vec<u8>) {
                output.push(#runtime::struct_pack::__private::TYPE_VARIANT);
                #(
                    <#variant_types as #runtime::StructPack>::append_type_literal(output);
                )*
                output.push(#runtime::struct_pack::__private::TYPE_END);
            }

            fn max_container_len(&self) -> usize {
                match self {
                    #(
                        Self::#variant_names(value) =>
                            <#variant_types as #runtime::StructPack>::max_container_len(value),
                    )*
                }
            }

            fn encode_payload(
                &self,
                encoder: &mut #runtime::struct_pack::Encoder<'_>,
            ) -> ::core::result::Result<(), #runtime::StructPackError> {
                match self {
                    #(
                        Self::#variant_names(value) => {
                            <u8 as #runtime::StructPack>::encode_payload(
                                &#variant_indices,
                                encoder,
                            )?;
                            <#variant_types as #runtime::StructPack>::encode_payload(value, encoder)
                        }
                    )*
                }
            }

            fn decode_payload(
                decoder: &mut #runtime::struct_pack::Decoder<'_>,
            ) -> ::core::result::Result<Self, #runtime::StructPackError> {
                let index = <u8 as #runtime::StructPack>::decode_payload(decoder)?;
                match index {
                    #(
                        #variant_indices => Ok(Self::#variant_names(
                            <#variant_types as #runtime::StructPack>::decode_payload(decoder)?,
                        )),
                    )*
                    index => Err(#runtime::StructPackError::InvalidVariantIndex {
                        name: stringify!(#name),
                        index,
                        alternatives: #alternative_count,
                    }),
                }
            }
        }
    })
}

fn generate_struct(
    structure: &Struct,
    runtime: &syn::Path,
    path: &Path,
) -> Result<TokenStream, CodegenError> {
    let name = rust_ident(&structure.name.to_upper_camel_case(), "struct", path)?;
    let fields = ordered_fields(&structure.fields);
    let field_names = fields
        .iter()
        .map(|field| rust_ident(&field.name.to_snake_case(), "field", path))
        .collect::<Result<Vec<_>, _>>()?;
    let field_types = fields
        .iter()
        .map(|field| rust_type(&field.type_, field.optional, runtime, path))
        .collect::<Result<Vec<_>, _>>()?;
    let struct_pack = if structure.cpp_u64_pair {
        quote! {
            impl #runtime::StructPack for #name {
                fn append_type_literal(output: &mut ::std::vec::Vec<u8>) {
                    output.push(#runtime::struct_pack::__private::TYPE_STRUCT);
                    #(
                        <#field_types as #runtime::StructPack>::append_type_literal(output);
                    )*
                    output.extend_from_slice(&[137, 137]);
                    output.push(#runtime::struct_pack::__private::TYPE_END);
                }

                fn max_container_len(&self) -> usize {
                    let mut maximum = 0;
                    #(
                        maximum = maximum.max(
                            <#field_types as #runtime::StructPack>::max_container_len(
                                &self.#field_names,
                            ),
                        );
                    )*
                    maximum
                }

                fn encode_payload(
                    &self,
                    encoder: &mut #runtime::struct_pack::Encoder<'_>,
                ) -> ::core::result::Result<(), #runtime::StructPackError> {
                    #(
                        <#field_types as #runtime::StructPack>::encode_payload(
                            &self.#field_names,
                            encoder,
                        )?;
                    )*
                    Ok(())
                }

                fn decode_payload(
                    decoder: &mut #runtime::struct_pack::Decoder<'_>,
                ) -> ::core::result::Result<Self, #runtime::StructPackError> {
                    Ok(Self {
                        #(
                            #field_names: <#field_types as #runtime::StructPack>::decode_payload(
                                decoder,
                            )?,
                        )*
                    })
                }
            }
        }
    } else {
        quote! {
            #runtime::impl_struct_pack!(#name {
                #(#field_names: #field_types,)*
            });
        }
    };
    Ok(quote! {
        #[derive(Debug, Clone, PartialEq)]
        pub struct #name {
            #(pub #field_names: #field_types,)*
        }

        #struct_pack
    })
}

fn generate_service(
    document: &Document,
    service: &Service,
    runtime: &syn::Path,
    path: &Path,
) -> Result<TokenStream, CodegenError> {
    let trait_name = rust_ident(&service.name.to_upper_camel_case(), "service", path)?;
    let client_name = format_ident!("{}Client", trait_name);
    let server_name = format_ident!("{}Server", trait_name);

    let schemas = TypeSchemas::new(document);

    let mut descriptors = Vec::new();
    let mut client_methods = Vec::new();
    let mut trait_methods = Vec::new();
    let mut registrations = Vec::new();

    for function in &service.functions {
        let method = rust_ident(&function.name.to_snake_case(), "method", path)?;
        let descriptor = format_ident!(
            "__{}_{}_METHOD",
            service.name.to_shouty_snake_case(),
            function.name.to_shouty_snake_case()
        );
        let wire_name = wire_name(document, service, function);
        let wire_name_literal = syn::LitStr::new(&wire_name, Span::call_site());
        let route_id = md5_hash32(wire_name.as_bytes());
        let return_type = rust_type(&function.returns, false, runtime, path)?;
        let mut stack = Vec::new();
        let response_hash = hash_type_literal(&type_literal(
            &function.returns,
            false,
            &schemas,
            &mut stack,
            path,
        )?);

        let fields = ordered_fields(&function.parameters);
        let parameter_names = fields
            .iter()
            .map(|field| rust_ident(&field.name.to_snake_case(), "parameter", path))
            .collect::<Result<Vec<_>, _>>()?;
        let parameter_types = fields
            .iter()
            .map(|field| rust_type(&field.type_, field.optional, runtime, path))
            .collect::<Result<Vec<_>, _>>()?;
        let request_type = request_rust_type(&parameter_types);

        if fields.is_empty() {
            descriptors.push(quote! {
                const #descriptor: #runtime::RpcNoArgsMethod<#return_type> =
                    #runtime::RpcNoArgsMethod::from_generated_parts(
                        #wire_name_literal,
                        #route_id,
                        #response_hash,
                    );
            });
        } else {
            let request_hash = hash_type_literal(&request_type_literal(
                &function.parameters,
                &schemas,
                &mut stack,
                path,
            )?);
            descriptors.push(quote! {
                #[allow(clippy::type_complexity)]
                const #descriptor: #runtime::RpcMethod<#request_type, #return_type> =
                    #runtime::RpcMethod::from_generated_parts(
                        #wire_name_literal,
                        #route_id,
                        #request_hash,
                        #response_hash,
                    );
            });
        }

        let parameters = quote! { #(#parameter_names: #parameter_types),* };
        let request_setup = if parameter_names.len() > 1 {
            quote! { let request = (#(#parameter_names,)*); }
        } else {
            TokenStream::new()
        };
        let request_ref = match parameter_names.as_slice() {
            [only] => quote! { &#only },
            _ => quote! { &request },
        };
        let client_call = if function.attachment {
            if parameter_names.is_empty() {
                quote! {
                    self.inner
                        .call_no_args_with_attachment(#descriptor, rpc_attachment)
                        .await
                }
            } else {
                quote! {
                    #request_setup
                    self.inner
                        .call_with_attachment(#descriptor, #request_ref, rpc_attachment)
                        .await
                }
            }
        } else if parameter_names.is_empty() {
            quote! { self.inner.call_no_args(#descriptor).await }
        } else {
            quote! {
                #request_setup
                self.inner.call(#descriptor, #request_ref).await
            }
        };
        let client_result = if function.attachment {
            quote! { #runtime::RpcReply<#return_type> }
        } else {
            quote! { #return_type }
        };
        let attachment_parameter = function
            .attachment
            .then(|| quote! { rpc_attachment: impl ::core::convert::Into<#runtime::Bytes> });
        let client_parameters = if let Some(attachment) = attachment_parameter {
            if parameter_names.is_empty() {
                quote! { #attachment }
            } else {
                quote! { #parameters, #attachment }
            }
        } else {
            parameters.clone()
        };
        let method_doc = format!("Calls the coro_rpc method `{wire_name}`.");
        client_methods.push(quote! {
            #[doc = #method_doc]
            pub async fn #method(
                &self,
                #client_parameters
            ) -> ::core::result::Result<#client_result, #runtime::RpcError> {
                #client_call
            }
        });

        let trait_result = if function.attachment {
            quote! { #runtime::RpcResponse<#return_type> }
        } else {
            quote! { #return_type }
        };
        let trait_parameters = if function.attachment {
            if parameter_names.is_empty() {
                quote! { rpc_context: #runtime::RequestContext }
            } else {
                quote! { rpc_context: #runtime::RequestContext, #parameters }
            }
        } else {
            parameters.clone()
        };
        trait_methods.push(quote! {
            #[doc = #method_doc]
            fn #method(
                &self,
                #trait_parameters
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<#trait_result, #runtime::RpcFailure>
            > + ::core::marker::Send;
        });

        let closure_pattern = match parameter_names.as_slice() {
            [only] => quote! { #only },
            _ => quote! { (#(#parameter_names,)*) },
        };
        let service_call = if function.attachment {
            quote! { service.#method(rpc_context, #(#parameter_names),*).await }
        } else {
            quote! { service.#method(#(#parameter_names),*).await }
        };
        let register_call = match (fields.is_empty(), function.attachment) {
            (true, false) => quote! {
                server.register_no_args(#descriptor, move || {
                    let service = ::std::sync::Arc::clone(&service);
                    async move { service.#method().await }
                })?;
            },
            (true, true) => quote! {
                server.register_no_args_with_context(#descriptor, move |rpc_context| {
                    let service = ::std::sync::Arc::clone(&service);
                    async move { service.#method(rpc_context).await }
                })?;
            },
            (false, false) => quote! {
                server.register(#descriptor, move |#closure_pattern| {
                    let service = ::std::sync::Arc::clone(&service);
                    async move { #service_call }
                })?;
            },
            (false, true) => quote! {
                server.register_with_context(#descriptor, move |#closure_pattern, rpc_context| {
                    let service = ::std::sync::Arc::clone(&service);
                    async move { #service_call }
                })?;
            },
        };
        registrations.push(quote! {
            {
                let service = ::std::sync::Arc::clone(&self.inner);
                #register_call
            }
        });
    }

    Ok(quote! {
        #(#descriptors)*

        /// Generated, cloneable client for this Thrift service.
        #[derive(Clone)]
        pub struct #client_name {
            inner: #runtime::RpcClient,
        }

        impl #client_name {
            pub fn new(inner: #runtime::RpcClient) -> Self {
                Self { inner }
            }

            pub async fn connect(
                address: impl #runtime::ToSocketAddrs,
            ) -> ::core::result::Result<Self, #runtime::RpcError> {
                Ok(Self::new(#runtime::RpcClient::connect(address).await?))
            }

            pub fn inner(&self) -> &#runtime::RpcClient {
                &self.inner
            }

            pub fn into_inner(self) -> #runtime::RpcClient {
                self.inner
            }

            #(#client_methods)*
        }

        /// Generated server interface for this Thrift service.
        pub trait #trait_name: ::core::marker::Send + ::core::marker::Sync + 'static {
            #(#trait_methods)*
        }

        /// Registers one implementation of the generated service interface.
        pub struct #server_name<S> {
            inner: ::std::sync::Arc<S>,
        }

        impl<S> #server_name<S> {
            pub fn new(inner: S) -> Self {
                Self {
                    inner: ::std::sync::Arc::new(inner),
                }
            }

            pub fn inner(&self) -> &S {
                &self.inner
            }
        }

        impl<S: #trait_name> #server_name<S> {
            pub fn register(
                &self,
                server: &mut #runtime::RpcServer,
            ) -> ::core::result::Result<(), #runtime::RegisterError> {
                #(#registrations)*
                Ok(())
            }

            pub fn into_rpc_server(
                self,
            ) -> ::core::result::Result<#runtime::RpcServer, #runtime::RegisterError> {
                let mut server = #runtime::RpcServer::new();
                self.register(&mut server)?;
                Ok(server)
            }

            pub fn into_rpc_server_with_config(
                self,
                config: #runtime::ServerConfig,
            ) -> ::core::result::Result<#runtime::RpcServer, #runtime::RegisterError> {
                let mut server = #runtime::RpcServer::with_config(config);
                self.register(&mut server)?;
                Ok(server)
            }
        }
    })
}

fn rust_type(
    type_: &Type,
    optional: bool,
    runtime: &syn::Path,
    path: &Path,
) -> Result<TokenStream, CodegenError> {
    let inner = match type_ {
        Type::Void => quote! { () },
        Type::Bool => quote! { bool },
        Type::I8 => quote! { i8 },
        Type::U8 => quote! { u8 },
        Type::I16 => quote! { i16 },
        Type::U16 => quote! { u16 },
        Type::I32 => quote! { i32 },
        Type::U32 => quote! { u32 },
        Type::I64 => quote! { i64 },
        Type::U64 => quote! { u64 },
        Type::F32 => quote! { f32 },
        Type::F64 => quote! { f64 },
        Type::String => quote! { ::std::string::String },
        Type::Binary => quote! { #runtime::ByteString },
        Type::List(value) => {
            let value = rust_type(value, false, runtime, path)?;
            quote! { ::std::vec::Vec<#value> }
        }
        Type::Set(value) => {
            let value = rust_type(value, false, runtime, path)?;
            quote! { ::std::collections::BTreeSet<#value> }
        }
        Type::Map(key, value) => {
            let key = rust_type(key, false, runtime, path)?;
            let value = rust_type(value, false, runtime, path)?;
            quote! { ::std::collections::BTreeMap<#key, #value> }
        }
        Type::Named(name) => {
            let name = rust_ident(&name.to_upper_camel_case(), "type reference", path)?;
            quote! { #name }
        }
    };
    Ok(if optional {
        quote! { ::core::option::Option<#inner> }
    } else {
        inner
    })
}

fn request_rust_type(parameter_types: &[TokenStream]) -> TokenStream {
    match parameter_types {
        [only] => quote! { #only },
        _ => quote! { (#(#parameter_types,)*) },
    }
}

fn ordered_fields(fields: &[Field]) -> Vec<&Field> {
    let mut ordered = fields.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|field| field.id);
    ordered
}

fn wire_name(document: &Document, service: &Service, function: &Function) -> String {
    if let Some(name) = &function.wire_name {
        return name.clone();
    }
    let namespace = service
        .namespace
        .as_deref()
        .or(document.cpp_namespace.as_deref())
        .unwrap_or("")
        .trim_matches(':');
    if namespace.is_empty() {
        function.name.clone()
    } else {
        format!("{namespace}::{}", function.name)
    }
}

fn request_type_literal(
    fields: &[Field],
    schemas: &TypeSchemas<'_>,
    stack: &mut Vec<String>,
    path: &Path,
) -> Result<Vec<u8>, CodegenError> {
    let fields = ordered_fields(fields);
    if let [field] = fields.as_slice() {
        return type_literal(&field.type_, field.optional, schemas, stack, path);
    }
    let mut literal = vec![TYPE_STRUCT];
    for field in fields {
        literal.extend(type_literal(
            &field.type_,
            field.optional,
            schemas,
            stack,
            path,
        )?);
    }
    literal.push(TYPE_END);
    Ok(literal)
}

fn type_literal(
    type_: &Type,
    optional: bool,
    schemas: &TypeSchemas<'_>,
    stack: &mut Vec<String>,
    path: &Path,
) -> Result<Vec<u8>, CodegenError> {
    let mut literal = if optional {
        vec![TYPE_OPTIONAL]
    } else {
        Vec::new()
    };
    match type_ {
        Type::Void => literal.push(TYPE_MONOSTATE),
        Type::Bool => literal.push(TYPE_BOOL),
        Type::I8 => literal.push(TYPE_INT8),
        Type::U8 => literal.push(TYPE_UINT8),
        Type::I16 => literal.push(TYPE_INT16),
        Type::U16 => literal.push(TYPE_UINT16),
        Type::I32 => literal.push(TYPE_INT32),
        Type::U32 => literal.push(TYPE_UINT32),
        Type::I64 => literal.push(TYPE_INT64),
        Type::U64 => literal.push(TYPE_UINT64),
        Type::F32 => literal.push(TYPE_FLOAT32),
        Type::F64 => literal.push(TYPE_FLOAT64),
        Type::String | Type::Binary => literal.extend([TYPE_STRING, TYPE_CHAR8]),
        Type::List(value) => {
            literal.push(TYPE_CONTAINER);
            literal.extend(type_literal(value, false, schemas, stack, path)?);
        }
        Type::Set(value) => {
            literal.push(TYPE_SET);
            literal.extend(type_literal(value, false, schemas, stack, path)?);
        }
        Type::Map(key, value) => {
            literal.push(TYPE_MAP);
            literal.extend(type_literal(key, false, schemas, stack, path)?);
            literal.extend(type_literal(value, false, schemas, stack, path)?);
        }
        Type::Named(name) => {
            if let Some(enumeration) = schemas.enums.get(name.as_str()) {
                literal.push(match enumeration.repr {
                    EnumRepr::I32 => TYPE_INT32,
                    EnumRepr::U8 => TYPE_UINT8,
                });
                return Ok(literal);
            }
            if stack.iter().any(|item| item == name) {
                return Err(invalid(
                    path,
                    format!("recursive type {name} is not supported by this struct_pack subset"),
                ));
            }
            stack.push(name.clone());
            if let Some(typedef) = schemas.typedefs.get(name.as_str()) {
                literal.extend(type_literal(&typedef.target, false, schemas, stack, path)?);
            } else if let Some(structure) = schemas.structs.get(name.as_str()) {
                literal.push(TYPE_STRUCT);
                for field in ordered_fields(&structure.fields) {
                    literal.extend(type_literal(
                        &field.type_,
                        field.optional,
                        schemas,
                        stack,
                        path,
                    )?);
                }
                if structure.cpp_u64_pair {
                    literal.extend([137, 137]);
                }
                literal.push(TYPE_END);
            } else if let Some(union) = schemas.unions.get(name.as_str()) {
                if union.expected {
                    let (value, error) = expected_alternatives(union, path)?;
                    literal.push(TYPE_EXPECTED);
                    if let Some(value) = value {
                        literal.extend(type_literal(&value.type_, false, schemas, stack, path)?);
                    } else {
                        literal.push(TYPE_MONOSTATE);
                    }
                    literal.extend(type_literal(&error.type_, false, schemas, stack, path)?);
                } else {
                    literal.push(TYPE_VARIANT);
                    for alternative in ordered_fields(&union.alternatives) {
                        literal.extend(type_literal(
                            &alternative.type_,
                            false,
                            schemas,
                            stack,
                            path,
                        )?);
                    }
                    literal.push(TYPE_END);
                }
            } else {
                return Err(invalid(path, format!("unknown type {name}")));
            }
            stack.pop();
        }
    }
    Ok(literal)
}

fn hash_type_literal(literal: &[u8]) -> u32 {
    md5_hash32(literal) & 0xffff_fffe
}

fn md5_hash32(input: &[u8]) -> u32 {
    let digest = Md5::digest(input);
    u32::from_be_bytes(digest[..4].try_into().expect("MD5 has at least four bytes"))
}

fn rust_ident(name: &str, description: &str, path: &Path) -> Result<Ident, CodegenError> {
    syn::parse_str::<Ident>(name)
        .or_else(|_| syn::parse_str::<Ident>(&format!("r#{name}")))
        .map_err(|_| {
            invalid(
                path,
                format!("{description} name {name:?} is not a Rust identifier"),
            )
        })
}

fn invalid(path: &Path, message: impl Into<String>) -> CodegenError {
    CodegenError::InvalidContract {
        path: path.to_owned(),
        message: message.into(),
    }
}
