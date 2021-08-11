pub(crate) mod json_multi {
    use mongodb::bson::Document;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub(crate) struct Tweet {
        text: String,
        in_reply_to_status_id: i64,
        retweet_count: Option<i32>,
        contributors: Option<i32>,
        created_at: String,
        geo: Option<String>,
        source: String,
        coordinates: Option<String>,
        in_reply_to_screen_name: Option<String>,
        truncated: bool,
        entities: Entities,
        retweeted: bool,
        place: Option<String>,
        user: User,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub(crate) struct Entities {
        user_mentions: Vec<Mention>,
        urls: Vec<String>,
        hashtags: Vec<String>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub(crate) struct Mention {
        indices: Vec<i32>,
        screen_name: String,
        name: String,
        id: i64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub(crate) struct User {
        friends_count: i32,
        profile_sidebar_fill_color: String,
        location: String,
        verified: bool,
        follow_request_sent: Option<bool>,
        favourites_count: i32,
        profile_sidebar_border_color: String,
        profile_image_url: String,
        geo_enabled: bool,
        created_at: String,
        description: String,
        time_zone: String,
        url: String,
        screen_name: String,
        notifications: Option<Vec<Document>>,
        profile_background_color: String,
        listed_count: i32,
        lang: String,
    }
}
