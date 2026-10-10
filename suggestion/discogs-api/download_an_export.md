## Download an export 
Download an export
GET
`/inventory/export/{id}/download`
Download the results of an inventory export.
  * **Parameters**
  * 

id
     `number` (required)
Id of the export.


  * **Response`200`** Toggle
  * ##### Headers
```
Content-Type: text/csv; charset=utf-8  
Content-Disposition: attachment; filename=cburmeister-inventory-20180927-1259.csv  

```

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
# Inventory Upload