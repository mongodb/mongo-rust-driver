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
        text("plot", "baseball").into()
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
            .into()
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
        .into()
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
        equals("verified_user", true).into()
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
        .into()
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
        .into()
    );
}
