pub(crate) mod tweet {
    use mongodb::bson::{Document, RawDocument};
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

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub(crate) struct TweetRef<'a> {
        text: &'a str,
        in_reply_to_status_id: i64,
        retweet_count: Option<i32>,
        contributors: Option<i32>,
        created_at: &'a str,
        geo: Option<&'a str>,
        source: &'a str,
        coordinates: Option<&'a str>,
        in_reply_to_screen_name: Option<&'a str>,
        truncated: bool,
        #[serde(borrow)]
        entities: EntitiesRef<'a>,
        retweeted: bool,
        place: Option<&'a str>,
        user: User,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub(crate) struct EntitiesRef<'a> {
        #[serde(borrow)]
        user_mentions: Vec<MentionRef<'a>>,
        urls: Vec<&'a str>,
        hashtags: Vec<&'a str>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub(crate) struct MentionRef<'a> {
        indices: Vec<i32>,
        screen_name: &'a str,
        name: &'a str,
        id: i64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub(crate) struct UserRef<'a> {
        friends_count: i32,
        profile_sidebar_fill_color: &'a str,
        location: &'a str,
        verified: bool,
        follow_request_sent: Option<bool>,
        favourites_count: i32,
        profile_sidebar_border_color: &'a str,
        profile_image_url: &'a str,
        geo_enabled: bool,
        created_at: &'a str,
        description: &'a str,
        time_zone: &'a str,
        url: &'a str,
        screen_name: &'a str,
        #[serde(borrow)]
        notifications: Option<Vec<&'a RawDocument>>,
        profile_background_color: &'a str,
        listed_count: i32,
        lang: &'a str,
    }
}
