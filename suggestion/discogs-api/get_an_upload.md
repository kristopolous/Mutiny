## Get an upload 
Get an upload
GET
`/inventory/upload/{id}`
Get details about the status of an inventory upload.
  * **Parameters**
  * 

id
     `number` (required)
Id of the export.


  * **Request** Toggle
  * ##### Headers
```
Content-Type: multipart/form-data  
If-Modified-Since: Thu, 27 Sep 2018 09:20:28 GMT  

```

  * **Response`200`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

##### Body
```
{
  "status": "success",
  "results": "CSV file contains 1 records.<p>Processed 1 records.",
  "created_ts": "2017-12-18T09:20:28",
  "finished_ts": "2017-12-18T09:20:29",
  "filename": "add.csv",
  "type": "change",
  "id": 119615
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



Next 
* * *
# User Identity