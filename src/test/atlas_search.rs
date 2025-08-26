use crate::{atlas_search, bson::doc};

#[test]
fn api_flow() {
    assert_eq!(
        doc! {
            "$search": {
                "autocomplete": {
                    "query": "pre",
                    "path": "title",
                    "fuzzy": {
                        "maxEdits": 1,
                        "prefixLength": 1,
                        "maxExpansions": 256,
                    },
                }
            }
        },
        atlas_search::autocomplete("title", "pre")
            .fuzzy(doc! { "maxEdits": 1, "prefixLength": 1, "maxExpansions": 256 })
            .into()
    );
    assert_eq!(
        doc! {
            "$search": {
                "text": {
                    "path": "plot",
                    "query": "baseball",
                }
            }
        },
        atlas_search::text("plot", "baseball").into()
    );
    assert_eq!(
        doc! {
            "$search": {
                "compound": {
                    "must": [{
                        "text": {
                            "path": "description",
                            "query": "varieties",
                        }
                    }],
                    "should": [{
                        "text": {
                            "path": "description",
                            "query": "Fuji",
                        }
                    }],
                }
            }
        },
        atlas_search::compound()
            .must(atlas_search::text("description", "varieties"))
            .should(atlas_search::text("description", "Fuji"))
            .into()
    );
    {
        use atlas_search::*;
        assert_eq!(
            doc! {
                "$search": {
                    "embeddedDocument": {
                        "path": "items",
                        "operator": {
                            "compound": {
                                "must": [{
                                    "text": {
                                        "path": "items.tags",
                                        "query": "school",
                                    }
                                }],
                                "should": [{
                                    "text": {
                                        "path": "items.name",
                                        "query": "backpack",
                                    }
                                }]
                            }
                        },
                        "score": {
                            "embedded": {
                                "aggregate": "mean"
                            }
                        },
                    }
                }
            },
            embedded_document(
                "items",
                compound()
                    .must(text("items.tags", "school"))
                    .should(text("items.name", "backpack")),
            )
            .score(doc! {
                "embedded": {
                    "aggregate": "mean"
                }
            })
            .into()
        );
    }
    assert_eq!(
        doc! {
            "$search": {
                "equals": {
                    "path": "verified_user",
                    "value": true,
                }
            }
        },
        atlas_search::equals("verified_user", true).into()
    );
    let gte_dt = crate::bson::DateTime::parse_rfc3339_str("2000-01-01T00:00:00.000Z").unwrap();
    let lte_dt = crate::bson::DateTime::parse_rfc3339_str("2015-01-31T00:00:00.000Z").unwrap();
    assert_eq!(
        doc! {
            "$searchMeta": {
                "facet": {
                    "operator": {
                        "range": {
                            "path": "released",
                            "gte": gte_dt,
                            "lte": lte_dt,
                        }
                    },
                    "facets": {
                        "directorsFacet": {
                            "type": "string",
                            "path": "directors",
                            "numBuckets": 7,
                        },
                        "yearFacet": {
                            "type": "number",
                            "path": "year",
                            "boundaries": [2000, 2005, 2010, 2015]
                        },
                    }
                }
            }
        },
        atlas_search::facet(doc! {
            "directorsFacet": {
                "type": "string",
                "path": "directors",
                "numBuckets": 7,
            },
            "yearFacet": {
                "type": "number",
                "path": "year",
                "boundaries": [2000, 2005, 2010, 2015]
            },
        })
        .operator(doc! {
            "range": {
                "path": "released",
                "gte": gte_dt,
                "lte": lte_dt,
            }
        })
        .into()
    );
}
