## Get recent exports 
Get recent exports
GET
`/inventory/export`
Get a list of all recent exports of your inventory. Accepts Pagination parameters.
  * **Response`200`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

##### Body
```
{
  "items": [
    {
      "status": "success",
      "created_ts": "2018-09-27T12:59:02",
      "url": "https://api.discogs.com/inventory/export/599632",
      "finished_ts": "2018-09-27T12:59:02",
      "download_url": "https://api.discogs.com/inventory/export/599632/download",
      "filename": "cburmeister-inventory-20180927-1259.csv",
      "id": 599632
    }
  ],
  "pagination": {
    "per_page": 50,
    "items": 15,
    "page": 1,
    "urls": {},
    "pages": 1
  }
}
```

  * **Response`401`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```