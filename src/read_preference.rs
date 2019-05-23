use std::{collections::HashMap, fmt, time::Duration};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ReadPreference {
    pub mode: Mode,
    pub max_staleness: Option<Duration>,
}

impl ReadPreference {
    pub fn primary() -> Self {
        ReadPreference {
            mode: Mode::Primary,
            ..Default::default()
        }
    }

    pub fn secondary(tag_sets: Option<Vec<TagSet>>) -> Self {
        ReadPreference {
            mode: Mode::Secondary(tag_sets.unwrap_or_default()),
            ..Default::default()
        }
    }

    pub fn primary_preferred(tag_sets: Option<Vec<TagSet>>) -> Self {
        ReadPreference {
            mode: Mode::PrimaryPreferred(tag_sets.unwrap_or_default()),
            ..Default::default()
        }
    }

    pub fn secondary_preferred(tag_sets: Option<Vec<TagSet>>) -> Self {
        ReadPreference {
            mode: Mode::SecondaryPreferred(tag_sets.unwrap_or_default()),
            ..Default::default()
        }
    }

    pub fn nearest(tag_sets: Option<Vec<TagSet>>) -> Self {
        ReadPreference {
            mode: Mode::Nearest(tag_sets.unwrap_or_default()),
            ..Default::default()
        }
    }

    pub(crate) fn is_primary(&self) -> bool {
        match self.mode {
            Mode::Primary => true,
            _ => false,
        }
    }

    pub(crate) fn is_secondary_preferred(&self) -> bool {
        match self.mode {
            Mode::SecondaryPreferred(_) => true,
            _ => false,
        }
    }
}

impl fmt::Display for ReadPreference {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.mode)?;

        if let Some(max_staleness) = self.max_staleness {
            write!(fmt, ", maxStalenessSeconds {}", max_staleness.as_secs())?;
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Mode {
    Primary,
    Secondary(Vec<TagSet>),
    PrimaryPreferred(Vec<TagSet>),
    SecondaryPreferred(Vec<TagSet>),
    Nearest(Vec<TagSet>),
}

impl Default for Mode {
    fn default() -> Self {
        Mode::Primary
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Mode::Primary => write!(fmt, "primary"),
            Mode::Secondary(tag_sets) => write!(fmt, "secondary, tag sets {:?}", tag_sets),
            Mode::PrimaryPreferred(tag_sets) => {
                write!(fmt, "primaryPreferred, tag sets {:?}", tag_sets)
            }
            Mode::SecondaryPreferred(tag_sets) => {
                write!(fmt, "secondaryPreferred, tag sets {:?}", tag_sets)
            }
            Mode::Nearest(tag_sets) => write!(fmt, "nearest, tag sets {:?}", tag_sets),
        }
    }
}

pub type TagSet = HashMap<String, String>;

#[macro_export]
macro_rules! tag_set {
    ( $($k:expr => $v:expr),* ) => {
        #[allow(clippy::let_and_return)]
        {
            use std::collections::HashMap;

            #[allow(unused_mut)]
            let mut ts = HashMap::new();
            $(
                ts.insert($k.to_string(), $v.to_string());
            )*

            ts
        }
    }
}
