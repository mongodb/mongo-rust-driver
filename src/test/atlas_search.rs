use crate::{
    atlas_search::*,
    bson::{doc, DateTime},
};

#[test]
fn helper_output_doc() {
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
        autocomplete("title", "pre")
            .fuzzy(doc! { "maxEdits": 1, "prefixLength": 1, "maxExpansions": 256 })
            .stage()
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
        text("plot", "baseball").stage()
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
        compound()
            .must(text("description", "varieties"))
            .should(text("description", "Fuji"))
            .stage()
    );
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
        .stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "equals": {
                    "path": "verified_user",
                    "value": true,
                }
            }
        },
        equals("verified_user", true).stage()
    );
    let gte_dt = DateTime::parse_rfc3339_str("2000-01-01T00:00:00.000Z").unwrap();
    let lte_dt = DateTime::parse_rfc3339_str("2015-01-31T00:00:00.000Z").unwrap();
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
        facet(doc! {
            "directorsFacet": facet::string("directors").num_buckets(7),
            "yearFacet": facet::number("year", [2000, 2005, 2010, 2015]),
        })
        .operator(range("released").gte(gte_dt).lte(lte_dt))
        .stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "geoShape": {
                    "relation": "disjoint",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[-161.323242,22.512557],
                                        [-152.446289,22.065278],
                                        [-156.09375,17.811456],
                                        [-161.323242,22.512557]]]
                    },
                    "path": "address.location"
                }
            }
        },
        geo_shape(
            "address.location",
            Relation::Disjoint,
            doc! {
                "type": "Polygon",
                "coordinates": [[[-161.323242,22.512557],
                                [-152.446289,22.065278],
                                [-156.09375,17.811456],
                                [-161.323242,22.512557]]]
            }
        )
        .stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "geoWithin": {
                    "path": "address.location",
                    "box": {
                        "bottomLeft": {
                            "type": "Point",
                            "coordinates": [112.467, -55.050]
                        },
                        "topRight": {
                            "type": "Point",
                            "coordinates": [168.000, -9.133]
                        }
                    }
                }
            }
        },
        geo_within("address.location")
            .geo_box(doc! {
                "bottomLeft": {
                    "type": "Point",
                    "coordinates": [112.467, -55.050]
                },
                "topRight": {
                    "type": "Point",
                    "coordinates": [168.000, -9.133]
                }
            })
            .stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "in": {
                    "path": "accounts",
                    "value": [371138, 371139, 371140]
                }
            }
        },
        search_in("accounts", [371138, 371139, 371140].as_ref()).stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "moreLikeThis": {
                    "like": {
                        "title": "The Godfather",
                        "genres": "action"
                    }
                }
            }
        },
        more_like_this(doc! {
            "title": "The Godfather",
            "genres": "action"
        })
        .stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "index": "runtimes",
                "near": {
                    "path": "year",
                    "origin": 2000,
                    "pivot": 2
                }
            }
        },
        search(near("year", 2000, 2)).index("runtimes").stage()
    );
    let dt = DateTime::parse_rfc3339_str("1915-09-13T00:00:00.000+00:00").unwrap();
    assert_eq!(
        doc! {
            "$search": {
                "index": "releaseddate",
                "near": {
                    "path": "released",
                    "origin": dt,
                    "pivot": 7776000000i64
                }
            }
        },
        search(near("released", dt, 7776000000i64))
            .index("releaseddate")
            .stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "near": {
                    "origin": {
                        "type": "Point",
                        "coordinates": [-8.61308, 41.1413]
                    },
                    "pivot": 1000,
                    "path": "address.location"
                }
            }
        },
        near(
            "address.location",
            doc! {
                "type": "Point",
                "coordinates": [-8.61308, 41.1413]
            },
            1000,
        )
        .stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "phrase": {
                    "path": "title",
                    "query": "new york"
                }
            }
        },
        phrase("title", "new york").stage()
    );
    #[cfg(feature = "bson-3")]
    assert_eq!(
        doc! {
            "$search": {
                "phrase": {
                    "path": "title",
                    "query": ["the man", "the moon"]
                }
            }
        },
        phrase("title", ["the man", "the moon"]).stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "queryString": {
                    "defaultPath": "title",
                    "query": "Rocky AND (IV OR 4 OR Four)"
                }
            }
        },
        query_string("title", "Rocky AND (IV OR 4 OR Four)").stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "regex": {
                    "path": "title",
                    "query": "(.*) Seattle"
                }
            }
        },
        regex("title", "(.*) Seattle").stage()
    );
    assert_eq!(
        doc! {
            "$search": {
                "wildcard": {
                    "query": "Green D*",
                    "path": "title"
                }
            }
        },
        wildcard("title", "Green D*").stage()
    );
}

#[test]
fn string_or_array_forms() {
    exists("hello");
    exists("hello".to_owned());
    #[cfg(feature = "bson-3")]
    exists(["hello", "world"]);
    exists(&["hello", "world"] as &[&str]);
    exists(&["hello".to_owned()] as &[String]);
}
