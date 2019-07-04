use super::Transform;
use crate::event::{self, Event};
use serde::{Deserialize, Serialize};
use string_cache::DefaultAtom as Atom;
use logfmt::{parse};
use std::str;

#[derive(Deserialize, Serialize, Debug, Clone, Derivative)]
#[serde(deny_unknown_fields, default)]
#[derivative(Default)]
pub struct LogfmtParserConfig {
    pub field: Option<Atom>,
    pub drop_invalid: bool,
    #[derivative(Default(value = "true"))]
    pub drop_field: bool,
}

#[typetag::serde(name = "logfmt_parser")]
impl crate::topology::config::TransformConfig for LogfmtParserConfig {
    fn build(&self) -> Result<Box<dyn Transform>, String> {
        Ok(Box::new(LogfmtParser::from(self.clone())))
    }
}

struct LogfmtParser {
    field: Atom,
    drop_invalid: bool,
    drop_field: bool,
}

impl From<LogfmtParserConfig> for LogfmtParser {
    fn from(config: LogfmtParserConfig) -> LogfmtParser {
        let field = if let Some(field) = &config.field {
            field
        } else {
            &event::MESSAGE
        };

        LogfmtParser {
            field: field.clone(),
            drop_invalid: config.drop_invalid,
            drop_field: config.drop_field,
        }
    }
}

impl Transform for LogfmtParser {
    fn transform(&mut self, mut event: Event) -> Option<Event> {
        let to_parse = event.as_log().get(&self.field).map(|s| s.to_string_lossy());

        let parsed = to_parse
            .and_then(|to_parse| {
                Some(parse(&to_parse.to_string()))
            });


        if let Some(object) = parsed {
            for p in object {
                insert(&mut event, p.key, p.val);
            }
        } else {
            if self.drop_invalid {
                return None;
            }
        }

        if self.drop_field {
            event.as_mut_log().remove(&self.field);
        }

        Some(event)
    }
}

fn insert(event: &mut Event, name: String, value: Option<String>) {
    match value {
        Some(value) => {
            event
                .as_mut_log()
                .insert_explicit(name.into(), value.into());
        },
        None => panic!("logfmt_parser: got None for value"),
    }
}
