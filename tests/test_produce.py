import subframe
from substrait.json import dump_json

data = subframe.table(
    [("a", "int64")],
    name="data",
)

ref = subframe.table(
    [("a", "int64")],
    name="ref",
)


def test_cte():
    out = dump_json(data.union(ref.view()).to_substrait())

    expected = """{
  "relations": [
    {
      "rel": {
        "read": {
          "common": {
            "direct": {}
          },
          "baseSchema": {
            "names": [
              "a"
            ],
            "struct": {
              "types": [
                {
                  "i64": {
                    "nullability": "NULLABILITY_NULLABLE"
                  }
                }
              ],
              "nullability": "NULLABILITY_NULLABLE"
            }
          },
          "namedTable": {
            "names": [
              "ref"
            ]
          }
        }
      }
    },
    {
      "root": {
        "input": {
          "set": {
            "inputs": [
              {
                "read": {
                  "common": {
                    "direct": {}
                  },
                  "baseSchema": {
                    "names": [
                      "a"
                    ],
                    "struct": {
                      "types": [
                        {
                          "i64": {
                            "nullability": "NULLABILITY_NULLABLE"
                          }
                        }
                      ],
                      "nullability": "NULLABILITY_NULLABLE"
                    }
                  },
                  "namedTable": {
                    "names": [
                      "data"
                    ]
                  }
                }
              },
              {
                "reference": {}
              }
            ],
            "op": "SET_OP_UNION_DISTINCT"
          }
        },
        "names": [
          "a"
        ]
      }
    }
  ],
  "version": {
    "minorNumber": 54,
    "producer": "subframe"
  }
}"""
    assert out == expected
