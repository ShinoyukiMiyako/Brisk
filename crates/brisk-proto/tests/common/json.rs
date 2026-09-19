//! Generated JSON for the property tests: a small value tree, a serializer
//! that varies whitespace and string escapes, and request strategies for
//! `ChatHead` and `plan_chat` (contract 05, 4.2).
//!
//! Whitespace and escape choices come from a seeded generator inside the
//! serializer rather than from proptest, which keeps the strategies small;
//! the seed is part of every case, so failures still reproduce.

use proptest::collection::vec;
use proptest::option;
use proptest::prelude::*;

use super::TEST_MODEL;
use super::oracle::{KeyClass, OPTIONS_FIELDS, TOP_FIELDS, classify};

/// A JSON value. Objects keep their members in order, duplicates included.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Json {
    Null,
    Bool(bool),
    /// The number's text, already valid JSON.
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

/// xorshift64: enough randomness for layout choices, reproducible from the
/// seed proptest generated.
#[derive(Debug, Clone)]
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        // xorshift has a fixed point at zero.
        Self(seed | 1)
    }

    pub(crate) fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform-enough value in `0..bound`; `bound` must not be zero.
    pub(crate) fn below(&mut self, bound: usize) -> usize {
        usize::try_from(self.next() % bound as u64).expect("below a usize bound")
    }

    pub(crate) fn one_in(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }

    pub(crate) fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            items.swap(i, self.below(i + 1));
        }
    }
}

/// How a value is written out.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Style {
    pub(crate) seed: u64,
    /// Put random JSON whitespace between tokens.
    pub(crate) whitespace: bool,
    /// Write some key characters as `\u` escapes (or `\/`).
    pub(crate) escape_keys: bool,
    /// Write some characters of string values as escapes.
    pub(crate) escape_strings: bool,
}

impl Style {
    pub(crate) const COMPACT: Self = Self {
        seed: 1,
        whitespace: false,
        escape_keys: false,
        escape_strings: false,
    };
}

pub(crate) fn style() -> impl Strategy<Value = Style> {
    (any::<u64>(), any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
        |(seed, whitespace, escape_keys, escape_strings)| Style {
            seed,
            whitespace,
            escape_keys,
            escape_strings,
        },
    )
}

/// Serializes `value` in the given style.
pub(crate) fn write(value: &Json, style: Style) -> Vec<u8> {
    let mut writer = Writer {
        out: Vec::new(),
        rng: Rng::new(style.seed),
        style,
    };
    writer.ws();
    writer.value(value);
    writer.ws();
    writer.out
}

struct Writer {
    out: Vec<u8>,
    rng: Rng,
    style: Style,
}

impl Writer {
    fn ws(&mut self) {
        if !self.style.whitespace {
            return;
        }
        for _ in 0..self.rng.below(3) {
            let byte = b" \n\t\r"[self.rng.below(4)];
            self.out.push(byte);
        }
    }

    fn value(&mut self, value: &Json) {
        match value {
            Json::Null => self.out.extend_from_slice(b"null"),
            Json::Bool(true) => self.out.extend_from_slice(b"true"),
            Json::Bool(false) => self.out.extend_from_slice(b"false"),
            Json::Num(text) => self.out.extend_from_slice(text.as_bytes()),
            Json::Str(text) => self.string(text, self.style.escape_strings),
            Json::Arr(items) => {
                self.out.push(b'[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        self.ws();
                        self.out.push(b',');
                    }
                    self.ws();
                    self.value(item);
                }
                self.ws();
                self.out.push(b']');
            }
            Json::Obj(members) => {
                self.out.push(b'{');
                for (index, (key, item)) in members.iter().enumerate() {
                    if index > 0 {
                        self.ws();
                        self.out.push(b',');
                    }
                    self.ws();
                    self.string(key, self.style.escape_keys);
                    self.ws();
                    self.out.push(b':');
                    self.ws();
                    self.value(item);
                }
                self.ws();
                self.out.push(b'}');
            }
        }
    }

    fn string(&mut self, text: &str, escape: bool) {
        self.out.push(b'"');
        for c in text.chars() {
            let short = match c {
                '"' => Some(b'"'),
                '\\' => Some(b'\\'),
                '\n' => Some(b'n'),
                '\r' => Some(b'r'),
                '\t' => Some(b't'),
                '\u{8}' => Some(b'b'),
                '\u{c}' => Some(b'f'),
                '/' if escape && self.rng.one_in(2) => Some(b'/'),
                _ => None,
            };
            if let Some(letter) = short {
                if self.rng.one_in(3) {
                    self.unicode_escape(c);
                } else {
                    self.out.extend_from_slice(&[b'\\', letter]);
                }
            } else if u32::from(c) < 0x20 || (escape && self.rng.one_in(3)) {
                self.unicode_escape(c);
            } else {
                let mut buf = [0; 4];
                self.out
                    .extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
            }
        }
        self.out.push(b'"');
    }

    fn unicode_escape(&mut self, c: char) {
        let mut units = [0; 2];
        for unit in c.encode_utf16(&mut units) {
            let text = if self.rng.one_in(2) {
                format!("\\u{unit:04x}")
            } else {
                format!("\\u{unit:04X}")
            };
            self.out.extend_from_slice(text.as_bytes());
        }
    }
}

/// Short strings: plain ASCII, arbitrary Unicode (control characters
/// included) and characters that must be escaped.
pub(crate) fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => "[a-z0-9_.()-]{0,10}",
        2 => vec(any::<char>(), 0..8).prop_map(String::from_iter),
        1 => Just(TEST_MODEL.to_owned()),
        1 => Just("\"\\/\u{0}\n\u{1f}".to_owned()),
        1 => Just("\u{6a21}\u{578b}\u{1f600}".to_owned()),
    ]
}

/// Non-empty model names, the default test model most often.
pub(crate) fn model_name() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => Just(TEST_MODEL.to_owned()),
        1 => Just("gpt-5.5".to_owned()),
        1 => Just("vendor/model \"quoted\" \\ tail".to_owned()),
        3 => text().prop_filter("model names are not empty", |name| !name.is_empty()),
    ]
}

fn number() -> impl Strategy<Value = String> {
    prop_oneof![
        any::<i64>().prop_map(|n| n.to_string()),
        any::<u64>().prop_map(|n| n.to_string()),
        (-1.0e6_f64..1.0e6).prop_map(|f| f.to_string()),
        Just("1.5e3".to_owned()),
        Just("-0".to_owned()),
        Just("0.0E-2".to_owned()),
    ]
}

/// Keys of generated objects. Recognized names and their case variants are
/// included on purpose: nested objects must be able to hold them.
pub(crate) fn key() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => "[a-z_]{1,12}",
        1 => Just("model".to_owned()),
        1 => Just("Model".to_owned()),
        1 => Just("stream".to_owned()),
        1 => Just("include_usage".to_owned()),
        1 => Just("messages".to_owned()),
        1 => Just("models".to_owned()),
        1 => Just("model_name".to_owned()),
        1 => text(),
    ]
}

/// Any JSON value up to a small depth.
pub(crate) fn json() -> impl Strategy<Value = Json> {
    let leaf = prop_oneof![
        Just(Json::Null),
        any::<bool>().prop_map(Json::Bool),
        number().prop_map(Json::Num),
        text().prop_map(Json::Str),
    ];
    leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            vec(inner.clone(), 0..4).prop_map(Json::Arr),
            vec((key(), inner), 0..4).prop_map(Json::Obj),
        ]
    })
}

/// A request body as members; `stream_options` is a `Json` so that it can
/// also be `null` or, for invalid requests, another type.
#[derive(Debug, Clone)]
pub(crate) struct Request {
    pub(crate) model: Option<Json>,
    pub(crate) stream: Option<Json>,
    pub(crate) stream_options: Option<Json>,
    pub(crate) extras: Vec<(String, Json)>,
    pub(crate) order: u64,
}

impl Request {
    /// The top-level members in a random order. Extra members whose key is
    /// a recognized field, or folds to one, are dropped: they would make the
    /// request a duplicate or an ambiguous one, which the tests inject
    /// deliberately instead.
    pub(crate) fn members(&self) -> Vec<(String, Json)> {
        let mut members: Vec<(String, Json)> = self
            .extras
            .iter()
            .filter(|(key, _)| classify(key, &TOP_FIELDS) == KeyClass::Other)
            .cloned()
            .collect();
        let declared = [
            ("model", &self.model),
            ("stream", &self.stream),
            ("stream_options", &self.stream_options),
        ];
        for (name, value) in declared {
            if let Some(value) = value {
                members.push((name.to_owned(), value.clone()));
            }
        }
        Rng::new(self.order).shuffle(&mut members);
        members
    }

    pub(crate) fn body(&self, style: Style) -> Vec<u8> {
        write(&Json::Obj(self.members()), style)
    }
}

/// A `stream_options` object whose `include_usage`, if any, sits at a random
/// position among other members.
fn options_object(include_usage: BoxedStrategy<Json>) -> impl Strategy<Value = Json> {
    (
        option::of(include_usage),
        vec((key(), json()), 0..3),
        any::<usize>(),
    )
        .prop_map(|(include_usage, extras, at)| {
            let mut members: Vec<(String, Json)> = extras
                .into_iter()
                .filter(|(key, _)| classify(key, &OPTIONS_FIELDS) == KeyClass::Other)
                .collect();
            if let Some(flag) = include_usage {
                let at = at % (members.len() + 1);
                members.insert(at, ("include_usage".to_owned(), flag));
            }
            Json::Obj(members)
        })
}

/// A `messages` array whose entries carry nested `model` keys, which must
/// never be taken for the top-level one.
fn messages() -> impl Strategy<Value = (String, Json)> {
    vec((text(), model_name()), 1..3).prop_map(|entries| {
        let items = entries
            .into_iter()
            .map(|(content, model)| {
                Json::Obj(vec![
                    ("role".to_owned(), Json::Str("user".to_owned())),
                    ("content".to_owned(), Json::Str(content)),
                    ("model".to_owned(), Json::Str(model)),
                    ("Model".to_owned(), Json::Null),
                ])
            })
            .collect();
        ("messages".to_owned(), Json::Arr(items))
    })
}

/// Requests. With `valid`, `ChatHead::parse` must accept every one: `model`
/// is a non-empty string, `stream` a boolean or `null`, `stream_options`
/// absent, `null` or an object whose `include_usage` is absent, `null` or a
/// boolean. Without it, each of those may also be missing or of a wrong type.
pub(crate) fn request(valid: bool) -> impl Strategy<Value = Request> {
    let flag = prop_oneof![any::<bool>().prop_map(Json::Bool), Just(Json::Null)];
    let (model, stream, stream_options) = if valid {
        (
            model_name().prop_map(|name| Some(Json::Str(name))).boxed(),
            option::of(flag.clone()).boxed(),
            option::of(prop_oneof![
                1 => Just(Json::Null),
                3 => options_object(flag.boxed()),
            ])
            .boxed(),
        )
    } else {
        let wrong = prop_oneof![
            Just(Json::Str("yes".to_owned())),
            Just(Json::Num("1".to_owned())),
            Just(Json::Arr(Vec::new())),
        ];
        let include_usage = prop_oneof![3 => flag.clone(), 1 => wrong.clone()].boxed();
        (
            option::of(prop_oneof![
                4 => text().prop_map(Json::Str),
                1 => Just(Json::Null),
                1 => number().prop_map(Json::Num),
            ])
            .boxed(),
            option::of(prop_oneof![3 => flag, 1 => wrong.clone()]).boxed(),
            option::of(prop_oneof![
                1 => Just(Json::Null),
                3 => options_object(include_usage),
                1 => wrong,
            ])
            .boxed(),
        )
    };
    (
        model,
        stream,
        stream_options,
        vec((key(), json()), 0..5),
        option::of(messages()),
        any::<u64>(),
    )
        .prop_map(
            |(model, stream, stream_options, mut extras, messages, order)| {
                extras.extend(messages);
                Request {
                    model,
                    stream,
                    stream_options,
                    extras,
                    order,
                }
            },
        )
}
