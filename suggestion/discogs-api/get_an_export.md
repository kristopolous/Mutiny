## Get an export 
Get an export
GET
`/inventory/export/{id}`
Get details about the status of an inventory export.
  * **Parameters**
  * 

id
     `number` (required)
Id of the export.


  * **Request** Toggle
  * ##### Headers
```
Content-Type: multipart/form-data  
If-Modified-Since: Thu, 27 Sep 2018 12:50:39 GMT  

```

  * **Response`200`** Toggle
  * ##### Headers
```
Content-Type: application/json  
Last-Modified: Thu, 27 Sep 2018 12:59:02 GMT  

```

##### Body
```
{
  "status": "success",
  "created_ts": "2018-09-27T12:50:39",
  "url": "https://api.discogs.com/inventory/export/599632",
  "finished_ts": "2018-09-27T12:59:02",
  "download_url": "https://api.discogs.com/inventory/export/599632/download",
  "filename": "cburmeister-inventory-20180927-1259.csv",
  "id": 599632
}
```

  * **Response`304`**
  * **Response`401`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`404`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```