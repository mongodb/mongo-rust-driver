use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

/// A collation configuration. See the official MongoDB
/// [documentation](https://docs.mongodb.com/manual/reference/collation/) for more information on
/// each of the fields.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Default, Serialize, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Collation {
    /// The ICU locale.
    ///
    /// See the list of supported languages and locales [here](https://docs.mongodb.com/manual/reference/collation-locales-defaults/#collation-languages-locales).
    pub locale: String,

    /// The level of comparison to perform. Corresponds to [ICU Comparison Levels](http://userguide.icu-project.org/collation/concepts#TOC-Comparison-Levels).
    #[builder(default, setter(strip_option))]
    pub strength: Option<i32>,

    /// Whether to include case comparison when `strength` is level 1 or 2.
    #[builder(default, setter(strip_option))]
    pub case_level: Option<bool>,

    /// The sort order of case differences during tertiary level comparisons.
    #[builder(default, setter(strip_option))]
    pub case_first: Option<String>,

    /// Whether to compare numeric strings as numbers or strings.
    #[builder(default, setter(strip_option))]
    pub numeric_ordering: Option<bool>,

    /// Whether collation should consider whitespace and punctuation as base characters for
    /// purposes of comparison.
    #[builder(default, setter(strip_option))]
    pub alternate: Option<String>,

    /// Up to which characters are considered ignorable when `alternate` is "shifted". Has no
    /// effect if `alternate` is set to "non-ignorable".
    #[builder(default, setter(strip_option))]
    pub max_variable: Option<String>,

    /// Whether to check if text require normalization and to perform it.
    #[builder(default, setter(strip_option))]
    pub normalization: Option<bool>,

    /// Whether strings with diacritics sort from the back of the string.
    #[builder(default, setter(strip_option))]
    pub backwards: Option<bool>,
}
