# Patent File Wrapper API

Examples for querying the USPTO [Open Data Portal (ODP)](https://data.uspto.gov/swagger/index.html) Patent File Wrapper search endpoint with `curl`.

The endpoint is:

```
https://api.uspto.gov/api/v1/patent/applications/search
```

It supports both `GET` (simplified syntax) and `POST` (advanced JSON syntax) requests. An API key is required and must be passed in the `X-API-KEY` header. You can [register and obtain an API key](https://data.uspto.gov/) on the USPTO website.

## Setup

Export your API key once per shell session:

```bash
export USPTO_API_KEY='your-api-key-here'
```

## Example: look up a single application by number

The following request searches for application `12057908` and returns up to 25 results starting at offset 0. This is the minimal query that returns a real result.

```bash
curl -sS -G 'https://api.uspto.gov/api/v1/patent/applications/search' \
  --data-urlencode 'q=applicationNumberText:12057908' \
  --data-urlencode 'limit=25' \
  --data-urlencode 'offset=0' \
  -H "X-API-KEY: ${USPTO_API_KEY}" \
  -H 'Accept: application/json'
```

Notes:

- `-G` tells `curl` to send the `--data-urlencode` parameters as a URL-encoded query string on a `GET` request.
- `q` is the search query. When a field name is given (`applicationNumberText:...`) the search is restricted to that field; without a field name the term is matched across all indexed fields.
- `limit` and `offset` paginate the results. The maximum `limit` is `100`.

## Example: simplified syntax (GET)

Search every indexed field for the word `Utility`:

```bash
curl -sS -G 'https://api.uspto.gov/api/v1/patent/applications/search' \
  --data-urlencode 'q=Utility' \
  -H "X-API-KEY: ${USPTO_API_KEY}" \
  -H 'Accept: application/json'
```

Search invention titles starting with `Apple` filed between 2021-01-01 and 2022-12-01:

```bash
curl -sS -G 'https://api.uspto.gov/api/v1/patent/applications/search' \
  --data-urlencode 'q=applicationMetaData.inventionTitle:Apple* AND applicationMetaData.filingDate:[2021-01-01 TO 2022-12-01]' \
  --data-urlencode 'offset=0' \
  --data-urlencode 'limit=25' \
  -H "X-API-KEY: ${USPTO_API_KEY}" \
  -H 'Accept: application/json'
```

## Example: advanced syntax (POST)

The advanced syntax uses a JSON body to combine `filters`, `rangeFilters`, `pagination`, `sort`, `facets`, and a `fields` projection. The query below searches for utility applications matching `Nanobody` with a filing date between 2022 and 2023, sorted by filing date descending.

```bash
curl -sS -X POST 'https://api.uspto.gov/api/v1/patent/applications/search' \
  -H "X-API-KEY: ${USPTO_API_KEY}" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '{
    "q": "Nanobody",
    "filters": [
      {
        "name": "applicationMetaData.applicationTypeLabelName",
        "value": ["Utility"]
      }
    ],
    "rangeFilters": [
      {
        "field": "applicationMetaData.filingDate",
        "valueFrom": "2022-01-01",
        "valueTo": "2023-12-31"
      }
    ],
    "pagination": {
      "offset": 0,
      "limit": 25
    },
    "sort": [
      {
        "field": "applicationMetaData.filingDate",
        "order": "Desc"
      }
    ],
    "fields": ["applicationNumberText", "applicationMetaData"]
  }'
```

## Tips

- The API is rate limited. On `HTTP 429` responses, back off briefly and retry.
- File downloads from related endpoints use HTTP 301 redirects; pass `-L` to `curl` to follow them.
- Pretty-print responses by piping into [`jq`](https://stedolan.github.io/jq/), e.g. `... | jq .patentFileWrapperDataBag[0]`.

## Reference

- Swagger UI: https://data.uspto.gov/swagger/index.html
- Open Data Portal home: https://data.uspto.gov/
