use bson::doc;

use crate::{error::Result, Database};

pub(crate) async fn populate(db: &Database) -> Result<()> {
    fn iso_date(text: &str) -> bson::DateTime {
        let chrono_dt = chrono::DateTime::parse_from_rfc3339(text).unwrap();
        bson::DateTime::from_millis(chrono_dt.timestamp_millis())
    }

    let date_20180208 = iso_date("2018-02-08T09:00:00.000Z");
    let date_20180109 = iso_date("2018-01-09T07:12:00.000Z");
    let date_20180127 = iso_date("2018-01-27T09:13:00.000Z");
    let date_20180203 = iso_date("2018-02-03T07:58:00.000Z");
    let date_20180205 = iso_date("2018-02-05T06:03:00.000Z");
    let date_20180111 = iso_date("2018-01-11T07:15:00.000Z");

    db.collection("sales")
        .insert_many(
            vec![
                doc! {
                    "date": date_20180208,
                    "items": [
                        doc! {
                            "fruit": "kiwi",
                            "quantity": 2,
                            "price": 0.5,
                        },
                        doc! {
                            "fruit": "apple",
                            "quantity": 1,
                            "price": 1.0,
                        },
                    ],
                },
                doc! {
                    "date": date_20180109,
                    "items": [
                        doc! {
                            "fruit": "banana",
                            "quantity": 8,
                            "price": 1.0,
                        },
                        doc! {
                            "fruit": "apple",
                            "quantity": 1,
                            "price": 1.0,
                        },
                        doc! {
                            "fruit": "papaya",
                            "quantity": 1,
                            "price": 4.0,
                        },
                    ],
                },
                doc! {
                    "date": date_20180127,
                    "items": [
                        doc! {
                            "fruit": "banana",
                            "quantity": 1,
                            "price": 1.0,
                        },
                    ],
                },
                doc! {
                    "date": date_20180203,
                    "items": [
                        doc! {
                            "fruit": "banana",
                            "quantity": 1,
                            "price": 1.0,
                        },
                    ],
                },
                doc! {
                    "date": date_20180205,
                    "items": [
                        doc! {
                            "fruit": "banana",
                            "quantity": 1,
                            "price": 1.0,
                        },
                        doc! {
                            "fruit": "mango",
                            "quantity": 2,
                            "price": 2.0,
                        },
                        doc! {
                            "fruit": "apple",
                            "quantity": 1,
                            "price": 1.0,
                        },
                    ],
                },
                doc! {
                    "date": date_20180111,
                    "items": [
                        doc! {
                            "fruit": "banana",
                            "quantity": 1,
                            "price": 1.0,
                        },
                        doc! {
                            "fruit": "apple",
                            "quantity": 1,
                            "price": 1.0,
                        },
                        doc! {
                            "fruit": "papaya",
                            "quantity": 3,
                            "price": 4.0,
                        },
                    ],
                },
            ],
            None,
        )
        .await?;
    db.collection("airlines")
        .insert_many(
            vec![
                doc! {
                    "airline": 17,
                    "name": "Air Canada",
                    "alias": "AC",
                    "iata": "ACA",
                    "icao": "AIR CANADA",
                    "active": "Y",
                    "country": "Canada",
                    "base": "TAL",
                },
                doc! {
                    "airline": 18,
                    "name": "Turkish Airlines",
                    "alias": "YK",
                    "iata": "TRK",
                    "icao": "TURKISH",
                    "active": "Y",
                    "country": "Turkey",
                    "base": "AET",
                },
                doc! {
                    "airline": 22,
                    "name": "Saudia",
                    "alias": "SV",
                    "iata": "SVA",
                    "icao": "SAUDIA",
                    "active": "Y",
                    "country": "Saudi Arabia",
                    "base": "JSU",
                },
                doc! {
                    "airline": 29,
                    "name": "Finnair",
                    "alias": "AY",
                    "iata": "FIN",
                    "icao": "FINNAIR",
                    "active": "Y",
                    "country": "Finland",
                    "base": "JMZ",
                },
                doc! {
                    "airline": 34,
                    "name": "Afric'air Express",
                    "alias": "",
                    "iata": "AAX",
                    "icao": "AFREX",
                    "active": "N",
                    "country": "Ivory Coast",
                    "base": "LOK",
                },
                doc! {
                    "airline": 37,
                    "name": "Artem-Avia",
                    "alias": "",
                    "iata": "ABA",
                    "icao": "ARTEM-AVIA",
                    "active": "N",
                    "country": "Ukraine",
                    "base": "JBR",
                },
                doc! {
                    "airline": 38,
                    "name": "Lufthansa",
                    "alias": "LH",
                    "iata": "DLH",
                    "icao": "LUFTHANSA",
                    "active": "Y",
                    "country": "Germany",
                    "base": "CYS",
                },
            ],
            None,
        )
        .await?;
    db.collection("air_alliances")
        .insert_many(
            vec![
                doc! {
                    "name": "Star Alliance",
                    "airlines": [
                        "Air Canada",
                        "Avianca",
                        "Air China",
                        "Air New Zealand",
                        "Asiana Airlines",
                        "Brussels Airlines",
                        "Copa Airlines",
                        "Croatia Airlines",
                        "EgyptAir",
                        "TAP Portugal",
                        "United Airlines",
                        "Turkish Airlines",
                        "Swiss International Air Lines",
                        "Lufthansa",
                    ],
                },
                doc! {
                    "name": "SkyTeam",
                    "airlines": [
                        "Aerolinias Argentinas",
                        "Aeromexico",
                        "Air Europa",
                        "Air France",
                        "Alitalia",
                        "Delta Air Lines",
                        "Garuda Indonesia",
                        "Kenya Airways",
                        "KLM",
                        "Korean Air",
                        "Middle East Airlines",
                        "Saudia",
                    ],
                },
                doc! {
                    "name": "OneWorld",
                    "airlines": [
                        "Air Berlin",
                        "American Airlines",
                        "British Airways",
                        "Cathay Pacific",
                        "Finnair",
                        "Iberia Airlines",
                        "Japan Airlines",
                        "LATAM Chile",
                        "LATAM Brasil",
                        "Malasya Airlines",
                        "Canadian Airlines",
                    ],
                },
            ],
            None,
        )
        .await?;

    Ok(())
}
