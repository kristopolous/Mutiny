## Get recent uploads 
Get recent uploads
GET
`/inventory/upload`
Get a list of all recent inventory uploads. Accepts Pagination parameters.
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
      "results": "CSV file contains 1 records.<p>Processed 1 records.",
      "created_ts": "2017-12-18T09:20:28",
      "finished_ts": "2017-12-18T09:20:29",
      "filename": "add.csv",
      "type": "change",
      "id": 119615
    }
  ],
  "pagination": {
    "per_page": 50,
    "items": 1,
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