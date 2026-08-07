use std::collections::BTreeMap;
use std::path::Path;

use arborium_tree_sitter::{Language, Node, Parser};

use crate::CodegenError;

#[derive(Debug)]
pub(crate) struct Document {
    pub rust_namespace: Vec<String>,
    pub cpp_namespace: Option<String>,
    pub typedefs: Vec<Typedef>,
    pub enums: Vec<Enum>,
    pub unions: Vec<Union>,
    pub structs: Vec<Struct>,
    pub services: Vec<Service>,
}

#[derive(Debug)]
pub(crate) struct Typedef {
    pub name: String,
    pub target: Type,
}

#[derive(Debug)]
pub(crate) struct Enum {
    pub name: String,
    pub variants: Vec<EnumVariant>,
    pub repr: EnumRepr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnumRepr {
    I32,
    U8,
}

#[derive(Debug)]
pub(crate) struct EnumVariant {
    pub name: String,
    pub value: i32,
}

#[derive(Debug)]
pub(crate) struct Union {
    pub name: String,
    pub alternatives: Vec<Field>,
    pub expected: bool,
}

#[derive(Debug)]
pub(crate) struct Struct {
    pub name: String,
    pub fields: Vec<Field>,
    pub cpp_u64_pair: bool,
}

#[derive(Debug)]
pub(crate) struct Service {
    pub name: String,
    pub namespace: Option<String>,
    pub functions: Vec<Function>,
}

#[derive(Debug)]
pub(crate) struct Function {
    pub name: String,
    pub wire_name: Option<String>,
    pub attachment: bool,
    pub returns: Type,
    pub parameters: Vec<Field>,
}

#[derive(Debug, Clone)]
pub(crate) struct Field {
    pub id: i64,
    pub name: String,
    pub type_: Type,
    pub optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Type {
    Void,
    Bool,
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    String,
    Binary,
    List(Box<Type>),
    Set(Box<Type>),
    Map(Box<Type>, Box<Type>),
    Named(String),
}

pub(crate) fn parse(source: &str, path: &Path) -> Result<Document, CodegenError> {
    let language = Language::new(arborium_thrift::language());
    let mut parser = Parser::new();
    parser
        .set_language(&language)
        .map_err(|error| invalid(path, format!("failed to load Thrift grammar: {error}")))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| invalid(path, "Tree-sitter returned no syntax tree"))?;
    let root = tree.root_node();
    if root.has_error() {
        let error = first_error(root).unwrap_or(root);
        let point = error.start_position();
        let fragment = text(error, source).trim();
        return Err(CodegenError::Parse {
            path: path.to_owned(),
            line: point.row + 1,
            column: point.column + 1,
            message: if fragment.is_empty() {
                "invalid or incomplete Thrift syntax".to_owned()
            } else {
                format!("invalid syntax near {fragment:?}")
            },
        });
    }

    let mut rust_namespace = Vec::new();
    let mut cpp_namespace = None;
    let mut typedefs = Vec::new();
    let mut enums = Vec::new();
    let mut unions = Vec::new();
    let mut structs = Vec::new();
    let mut services = Vec::new();

    for node in named_children(root) {
        let item = only_named_child(node).unwrap_or(node);
        match item.kind() {
            "namespace_declaration" => {
                let (scope, name) = parse_namespace(item, source, path)?;
                match scope.as_str() {
                    "rs" | "rust" => {
                        rust_namespace = name
                            .split('.')
                            .filter(|part| !part.is_empty())
                            .map(ToOwned::to_owned)
                            .collect();
                    }
                    "cpp" | "cpp2" => cpp_namespace = Some(name.replace('.', "::")),
                    "*" if rust_namespace.is_empty() => {
                        rust_namespace = name
                            .split('.')
                            .filter(|part| !part.is_empty())
                            .map(ToOwned::to_owned)
                            .collect();
                    }
                    _ => {}
                }
            }
            "include_statement" | "package_declaration" => {
                return Err(invalid(
                    path,
                    format!("{} is not supported yet", item.kind()),
                ));
            }
            "typedef_definition" => typedefs.push(parse_typedef(item, source, path)?),
            "enum_definition" => enums.push(parse_enum(item, source, path)?),
            "union_definition" => unions.push(parse_union(item, source, path)?),
            "struct_definition" => structs.push(parse_struct(item, source, path)?),
            "service_definition" => services.push(parse_service(item, source, path)?),
            "const_definition" => {
                return Err(invalid(
                    path,
                    "const definitions are not used by coro_rpc code generation",
                ));
            }
            "senum_definition" | "exception_definition" | "interaction_definition" => {
                return Err(invalid(
                    path,
                    format!("{} is not supported by the struct_pack subset", item.kind()),
                ));
            }
            kind => {
                return Err(invalid(
                    path,
                    format!("unexpected top-level Thrift node {kind}"),
                ));
            }
        }
    }

    if services.is_empty() {
        return Err(invalid(path, "the IDL must define at least one service"));
    }
    Ok(Document {
        rust_namespace,
        cpp_namespace,
        typedefs,
        enums,
        unions,
        structs,
        services,
    })
}

fn parse_union(node: Node<'_>, source: &str, path: &Path) -> Result<Union, CodegenError> {
    let name = node
        .child_by_field_name("type")
        .map(|node| text(node, source).to_owned())
        .ok_or_else(|| invalid(path, "union has no name"))?;
    let alternatives = named_children(node)
        .into_iter()
        .filter(|child| child.kind() == "field")
        .map(|field| {
            if named_children(field)
                .iter()
                .any(|child| child.kind() == "field_modifier")
            {
                return Err(invalid(
                    path,
                    format!(
                        "union {name} alternatives must not use required or optional modifiers"
                    ),
                ));
            }
            parse_field(field, source, path, "union alternative")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let annotations = parse_annotations(node, source, path)?;
    let expected = annotation_bool(&annotations, "coro_rpc.expected", path)?;
    Ok(Union {
        name,
        alternatives,
        expected,
    })
}

fn parse_enum(node: Node<'_>, source: &str, path: &Path) -> Result<Enum, CodegenError> {
    let name_node = node
        .child_by_field_name("type")
        .ok_or_else(|| invalid(path, "enum has no name"))?;
    let name = text(name_node, source).to_owned();
    let mut raw_variants = Vec::<(String, Option<i32>)>::new();

    for child in named_children(node) {
        match child.kind() {
            "identifier" if child.start_byte() != name_node.start_byte() => {
                raw_variants.push((text(child, source).to_owned(), None));
            }
            "number" => {
                let (_, value) = raw_variants.last_mut().ok_or_else(|| {
                    invalid(path, format!("enum {name} has a value without a variant"))
                })?;
                *value = Some(parse_enum_value(text(child, source), &name, path)?);
            }
            _ => {}
        }
    }

    let mut previous: Option<i32> = None;
    let variants = raw_variants
        .into_iter()
        .map(|(variant, explicit)| {
            let value = match explicit {
                Some(value) => value,
                None => match previous {
                    None => 0,
                    Some(value) => value.checked_add(1).ok_or_else(|| {
                        invalid(
                            path,
                            format!(
                                "implicit value for enum variant {name}::{variant} exceeds i32"
                            ),
                        )
                    })?,
                },
            };
            previous = Some(value);
            Ok(EnumVariant {
                name: variant,
                value,
            })
        })
        .collect::<Result<Vec<_>, CodegenError>>()?;

    let annotations = parse_annotations(node, source, path)?;
    let repr = match annotation_string(&annotations, "coro_rpc.repr", path)?.as_deref() {
        None | Some("i32") => EnumRepr::I32,
        Some("u8") => EnumRepr::U8,
        Some(repr) => {
            return Err(invalid(
                path,
                format!("annotation coro_rpc.repr supports i32 or u8, got {repr:?}"),
            ));
        }
    };
    Ok(Enum {
        name,
        variants,
        repr,
    })
}

fn parse_enum_value(raw: &str, enum_name: &str, path: &Path) -> Result<i32, CodegenError> {
    let compact = raw.replace('_', "");
    let (negative, unsigned) = match compact.as_bytes().first() {
        Some(b'-') => (true, &compact[1..]),
        Some(b'+') => (false, &compact[1..]),
        _ => (false, compact.as_str()),
    };
    let (radix, digits) = if let Some(value) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        (16, value)
    } else if let Some(value) = unsigned
        .strip_prefix("0b")
        .or_else(|| unsigned.strip_prefix("0B"))
    {
        (2, value)
    } else {
        (10, unsigned)
    };
    let magnitude = i64::from_str_radix(digits, radix).map_err(|_| {
        invalid(
            path,
            format!("enum {enum_name} value {raw:?} is not a valid integer"),
        )
    })?;
    let value = if negative { -magnitude } else { magnitude };
    i32::try_from(value).map_err(|_| {
        invalid(
            path,
            format!("enum {enum_name} value {raw:?} is outside the i32 range"),
        )
    })
}

fn parse_namespace(
    node: Node<'_>,
    source: &str,
    path: &Path,
) -> Result<(String, String), CodegenError> {
    let children = named_children(node);
    let scope = children
        .iter()
        .find(|child| child.kind() == "namespace_scope")
        .map(|node| text(*node, source).to_owned())
        .ok_or_else(|| invalid(path, "namespace has no scope"))?;
    let name = if let Some(string) = children.iter().find(|child| child.kind() == "string") {
        parse_string_or_raw(text(*string, source), path)?
    } else {
        let components = children
            .iter()
            .filter(|child| matches!(child.kind(), "namespace" | "identifier"))
            .map(|node| text(*node, source))
            .collect::<Vec<_>>();
        if components.is_empty() {
            return Err(invalid(path, "namespace has no name"));
        }
        components.join(".")
    };
    Ok((scope, name))
}

fn parse_typedef(node: Node<'_>, source: &str, path: &Path) -> Result<Typedef, CodegenError> {
    let children = named_children(node);
    let target = children
        .iter()
        .find(|child| child.kind() == "definition_type")
        .copied()
        .ok_or_else(|| invalid(path, "typedef has no target type"))?;
    let name = children
        .iter()
        .rev()
        .find(|child| child.kind() == "typedef_identifier")
        .map(|node| text(*node, source).to_owned())
        .ok_or_else(|| invalid(path, "typedef has no name"))?;
    Ok(Typedef {
        name,
        target: parse_type(target, source, path)?,
    })
}

fn parse_struct(node: Node<'_>, source: &str, path: &Path) -> Result<Struct, CodegenError> {
    let name = node
        .child_by_field_name("type")
        .map(|node| text(node, source).to_owned())
        .ok_or_else(|| invalid(path, "struct has no name"))?;
    let fields = named_children(node)
        .into_iter()
        .filter(|child| child.kind() == "field")
        .map(|field| parse_field(field, source, path, "struct field"))
        .collect::<Result<Vec<_>, _>>()?;
    let annotations = parse_annotations(node, source, path)?;
    let cpp_u64_pair = annotation_bool(&annotations, "coro_rpc.cpp_u64_pair", path)?;
    Ok(Struct {
        name,
        fields,
        cpp_u64_pair,
    })
}

fn parse_service(node: Node<'_>, source: &str, path: &Path) -> Result<Service, CodegenError> {
    let mut cursor = node.walk();
    let names = node
        .children_by_field_name("type", &mut cursor)
        .map(|node| text(node, source).to_owned())
        .collect::<Vec<_>>();
    let Some(name) = names.first().cloned() else {
        return Err(invalid(path, "service has no name"));
    };
    if names.len() != 1 {
        return Err(invalid(path, "service inheritance is not supported"));
    }
    let annotations = parse_annotations(node, source, path)?;
    let namespace = annotation_string(&annotations, "coro_rpc.namespace", path)?;
    let functions = named_children(node)
        .into_iter()
        .filter(|child| child.kind() == "function_definition")
        .map(|function| parse_function(function, source, path))
        .collect::<Result<Vec<_>, _>>()?;
    if functions.is_empty() {
        return Err(invalid(path, format!("service {name} has no functions")));
    }
    Ok(Service {
        name,
        namespace,
        functions,
    })
}

fn parse_function(node: Node<'_>, source: &str, path: &Path) -> Result<Function, CodegenError> {
    let children = named_children(node);
    if let Some(modifier) = children
        .iter()
        .find(|child| child.kind() == "function_modifier")
    {
        return Err(invalid(
            path,
            format!(
                "function modifier {:?} is not supported",
                text(*modifier, source)
            ),
        ));
    }
    if children.iter().any(|child| child.kind() == "throws") {
        return Err(invalid(
            path,
            "typed Thrift throws cannot be represented by coro_rpc v0 errors",
        ));
    }
    let returns = children
        .iter()
        .find(|child| child.kind() == "type")
        .copied()
        .map(|node| parse_type(node, source, path))
        .transpose()?
        .ok_or_else(|| invalid(path, "function has no return type"))?;
    let name = children
        .iter()
        .find(|child| child.kind() == "identifier")
        .map(|node| text(*node, source).to_owned())
        .ok_or_else(|| invalid(path, "function has no name"))?;
    let parameters = children
        .iter()
        .find(|child| child.kind() == "parameters")
        .map(|parameters| {
            named_children(*parameters)
                .into_iter()
                .filter(|child| child.kind() == "parameter")
                .map(|parameter| parse_field(parameter, source, path, "parameter"))
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let annotations = parse_annotations(node, source, path)?;
    let wire_name = annotation_string(&annotations, "coro_rpc.name", path)?;
    let attachment = annotation_bool(&annotations, "coro_rpc.attachment", path)?;
    Ok(Function {
        name,
        wire_name,
        attachment,
        returns,
        parameters,
    })
}

fn parse_field(
    node: Node<'_>,
    source: &str,
    path: &Path,
    description: &str,
) -> Result<Field, CodegenError> {
    let children = named_children(node);
    let id_node = children
        .iter()
        .find(|child| child.kind() == "field_id")
        .ok_or_else(|| invalid(path, format!("every {description} must have a field ID")))?;
    let id_text = text(*id_node, source).trim().trim_end_matches(':').trim();
    let id = id_text
        .parse::<i64>()
        .map_err(|_| invalid(path, format!("invalid field ID {id_text:?}")))?;
    if id <= 0 {
        return Err(invalid(path, "field IDs must be positive"));
    }
    let modifier = children
        .iter()
        .find(|child| child.kind() == "field_modifier")
        .map(|node| text(*node, source));
    let optional = modifier == Some("optional");
    let type_node = children
        .iter()
        .find(|child| child.kind() == "type")
        .copied()
        .ok_or_else(|| invalid(path, format!("{description} has no type")))?;
    let name = children
        .iter()
        .find(|child| child.kind() == "identifier")
        .map(|node| text(*node, source).to_owned())
        .ok_or_else(|| invalid(path, format!("{description} has no name")))?;
    if children.iter().any(|child| child.kind() == "literal") {
        return Err(invalid(
            path,
            format!("default values are not supported for {description} {name}"),
        ));
    }
    Ok(Field {
        id,
        name,
        type_: parse_type(type_node, source, path)?,
        optional,
    })
}

fn parse_type(node: Node<'_>, source: &str, path: &Path) -> Result<Type, CodegenError> {
    match node.kind() {
        "type" | "definition_type" | "container_type" => {
            let raw = text(node, source).trim();
            if raw == "void" {
                return Ok(Type::Void);
            }
            let child = named_children(node)
                .into_iter()
                .find(|child| {
                    matches!(
                        child.kind(),
                        "primitive" | "identifier" | "container_type" | "list" | "set" | "map"
                    )
                })
                .ok_or_else(|| invalid(path, format!("cannot understand type {raw:?}")))?;
            parse_type(child, source, path)
        }
        "primitive" => match text(node, source).trim() {
            "bool" => Ok(Type::Bool),
            "byte" | "i8" => Ok(Type::I8),
            "i16" => Ok(Type::I16),
            "i32" => Ok(Type::I32),
            "i64" => Ok(Type::I64),
            "float" => Ok(Type::F32),
            "double" => Ok(Type::F64),
            "string" => Ok(Type::String),
            "binary" => Ok(Type::Binary),
            unsupported => Err(invalid(
                path,
                format!("Thrift primitive {unsupported:?} is not supported"),
            )),
        },
        "identifier" => Ok(match text(node, source) {
            "u8" => Type::U8,
            "u16" => Type::U16,
            "u32" => Type::U32,
            "u64" => Type::U64,
            name => Type::Named(name.to_owned()),
        }),
        "list" | "set" => {
            let value = named_children(node)
                .into_iter()
                .find(|child| child.kind() == "type")
                .ok_or_else(|| invalid(path, format!("{} has no element type", node.kind())))?;
            let value = Box::new(parse_type(value, source, path)?);
            Ok(if node.kind() == "list" {
                Type::List(value)
            } else {
                Type::Set(value)
            })
        }
        "map" => {
            let types = named_children(node)
                .into_iter()
                .filter(|child| child.kind() == "type")
                .map(|child| parse_type(child, source, path))
                .collect::<Result<Vec<_>, _>>()?;
            if types.len() != 2 {
                return Err(invalid(path, "map must have exactly two types"));
            }
            Ok(Type::Map(
                Box::new(types[0].clone()),
                Box::new(types[1].clone()),
            ))
        }
        kind => Err(invalid(path, format!("unexpected type node {kind}"))),
    }
}

fn parse_annotations(
    node: Node<'_>,
    source: &str,
    path: &Path,
) -> Result<BTreeMap<String, Option<String>>, CodegenError> {
    let mut result = BTreeMap::new();
    for annotations in named_children(node)
        .into_iter()
        .filter(|child| child.kind() == "annotation_definition")
    {
        let children = named_children(annotations);
        let mut index = 0;
        while index < children.len() {
            if children[index].kind() != "annotation_identifier" {
                index += 1;
                continue;
            }
            let key = text(children[index], source).trim().to_owned();
            let value = children.get(index + 1).and_then(|candidate| {
                (candidate.kind() == "literal").then(|| text(*candidate, source).trim())
            });
            let value = value
                .map(|raw| parse_string_or_raw(raw, path))
                .transpose()?;
            result.insert(key, value);
            index += if children
                .get(index + 1)
                .is_some_and(|node| node.kind() == "literal")
            {
                2
            } else {
                1
            };
        }
    }
    Ok(result)
}

fn annotation_string(
    annotations: &BTreeMap<String, Option<String>>,
    key: &str,
    path: &Path,
) -> Result<Option<String>, CodegenError> {
    annotations
        .get(key)
        .map(|value| {
            value
                .clone()
                .ok_or_else(|| invalid(path, format!("annotation {key} requires a value")))
        })
        .transpose()
}

fn annotation_bool(
    annotations: &BTreeMap<String, Option<String>>,
    key: &str,
    path: &Path,
) -> Result<bool, CodegenError> {
    match annotations.get(key) {
        None => Ok(false),
        Some(None) => Ok(true),
        Some(Some(value)) if value == "true" => Ok(true),
        Some(Some(value)) if value == "false" => Ok(false),
        Some(Some(value)) => Err(invalid(
            path,
            format!("annotation {key} expects true or false, got {value:?}"),
        )),
    }
}

fn parse_string_or_raw(raw: &str, path: &Path) -> Result<String, CodegenError> {
    if raw.starts_with('"') {
        serde_json::from_str(raw)
            .map_err(|error| invalid(path, format!("invalid string literal {raw:?}: {error}")))
    } else if raw.starts_with('\'') {
        Err(invalid(
            path,
            "single-quoted annotation values are not supported; use double quotes",
        ))
    } else {
        Ok(raw.to_owned())
    }
}

fn named_children(node: Node<'_>) -> Vec<Node<'_>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor).collect()
}

fn only_named_child(node: Node<'_>) -> Option<Node<'_>> {
    (node.named_child_count() == 1)
        .then(|| node.named_child(0))
        .flatten()
}

fn first_error(node: Node<'_>) -> Option<Node<'_>> {
    if node.is_error() || node.is_missing() {
        return Some(node);
    }
    named_children(node).into_iter().find_map(first_error)
}

fn text<'a>(node: Node<'_>, source: &'a str) -> &'a str {
    node.utf8_text(source.as_bytes())
        .expect("the IDL source is valid UTF-8")
}

fn invalid(path: &Path, message: impl Into<String>) -> CodegenError {
    CodegenError::InvalidContract {
        path: path.to_owned(),
        message: message.into(),
    }
}
