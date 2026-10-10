## Delete inventory 
Delete inventory
POST
`/inventory/upload/delete`
Upload a CSV of listings to delete from your inventory.
### File structure ¶
The file you upload must be a comma-separated CSV. The first row must be a header with lower case field names.
Here’s an example:
```
listing_id
12345678
98765432
31415926
```

### Required fields ¶
  * `listing_id` - Must be a number. This is the ID of the listing you wish to delete.


  * **Parameters**
  * 

upload
     `file` (required)
The CSV file listing items to remove from your inventory.


  * **Response`200`** Toggle
  * ##### Headers
```
Content-Type: application/json  
Location: https://api.discogs.com/inventory/upload/599632  

```

  * **Response`401`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`409`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`415`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`422`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```