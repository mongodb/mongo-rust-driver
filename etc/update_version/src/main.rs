use std::{collections::HashMap, path::Path};

use regex::Regex;

#[derive(Debug)]
struct Location {
    path: &'static Path,
    pattern: Regex, // must contain a (?<target>) match group
}

impl Location {
    fn new(path: &'static str, pattern: &str) -> Self {
        Self {
            path: Path::new(path),
            pattern: Regex::new(pattern).unwrap(),
        }
    }
}

struct PendingUpdates {
    files: HashMap<&'static Path, String>,
}

impl PendingUpdates {
    fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    fn apply(&mut self, location: &Location, update: &str) {
        let text = self.files.entry(location.path).or_insert_with(|| {
            std::fs::read_to_string(location.path).expect(
                std::path::absolute(location.path)
                    .unwrap()
                    .to_str()
                    .unwrap(),
            )
        });

        if !location.pattern.is_match(text) {
            panic!("no match for {:?}", location);
        }
        let mut new_text = String::new();
        let mut last_match = 0;
        for caps in location.pattern.captures_iter(text) {
            let target = caps.name("target").expect("<target> capture group");
            let prefix = &text[last_match..target.start()];
            new_text.push_str(prefix);
            new_text.push_str(update);
            last_match = target.end();
        }
        new_text.push_str(&text[last_match..]);
        *text = new_text;
    }

    fn write(self) {
        for (path, contents) in self.files {
            std::fs::write(path, contents).unwrap();
        }
    }
}

#[derive(argh::FromArgs)]
/// Update crate and git dependency versions in prep for release.
struct Args {
    /// new version of the mongodb crate
    #[argh(option)]
    version: String,

    /// version of the bson crate (2.x)
    #[argh(option)]
    bson2: Option<String>,

    /// version of the bson crate (3.x)
    #[argh(option)]
    bson3: Option<String>,

    /// version of the mongocrypt crate
    #[argh(option)]
    mongocrypt: Option<String>,
}

fn main() {
    // nosemgrep: current-exe
    let zero = std::env::current_exe().unwrap();
    let self_dir = zero.parent().unwrap();
    let main_dir = self_dir.join("..").join("..");
    std::env::set_current_dir(main_dir).unwrap();

    let args: Args = argh::from_env();

    let version_locs = vec![
        Location::new(
            "driver/Cargo.toml",
            r#"name = "mongodb"\nversion = "(?<target>.*?)"\n"#,
        ),
        Location::new(
            "driver/Cargo.toml",
            r#"mongodb-internal-macros = \{ path = "../macros", version = "(?<target>.*?)" \}\n"#,
        ),
        Location::new(
            "macros/Cargo.toml",
            r#"name = "mongodb-internal-macros"\nversion = "(?<target>.*?)"\n"#,
        ),
        Location::new("README.md", r#"mongodb = "(?<target>.*?)"\n"#),
        Location::new(
            "README.md",
            r#"\[dependencies.mongodb\]\nversion = "(?<target>.*?)"\n"#,
        ),
        Location::new(
            "driver/src/lib.rs",
            r#"html_root_url = "https://docs.rs/mongodb/(?<target>.*?)""#,
        ),
    ];
    let mut pending = PendingUpdates::new();
    for loc in &version_locs {
        pending.apply(loc, &args.version);
    }

    if let Some(bson2) = args.bson2 {
        let bson_version_loc = Location::new(
            "driver/Cargo.toml",
            r#"\[dependencies.bson2\]\nversion = "(?<target>.*?)"\n"#,
        );
        pending.apply(&bson_version_loc, &bson2);
    }

    if let Some(bson3) = args.bson3 {
        let bson_version_loc = Location::new(
            "driver/Cargo.toml",
            r#"\[dependencies.bson3\]\nversion = "(?<target>.*?)"\n"#,
        );
        pending.apply(&bson_version_loc, &bson3);
    }

    if let Some(mongocrypt) = args.mongocrypt {
        let mongocrypt_version_loc = Location::new(
            "driver/Cargo.toml",
            r#"\[dependencies.mongocrypt\]\nversion = "(?<target>.*?)".*"#,
        );
        pending.apply(&mongocrypt_version_loc, &mongocrypt);
    }
    pending.write();
}
