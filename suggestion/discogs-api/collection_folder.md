## Collection Folder 
Get Folders
GET
`/users/{username}/collection/folders/{folder_id}`
Retrieve metadata about a folder in a user’s collection.
If `folder_id` is not `0`, authentication as the collection owner is required.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to request. 

folder_id
     `number` (required) **Example:** 3
The ID of the folder to request.


  * **Response`200`** Toggle
  * ##### Headers
```
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 132  
Date: Wed, 16 Jul 2014 23:20:21 GMT  
X-Varnish: 1722533701  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "id": 1,
  "count": 20,
  "name": "Uncategorized",
  "resource_url": "https://api.discogs.com/users/example/collection/folders/1"
}
```



Edit Folder
POST
`/users/{username}/collection/folders/{folder_id}`
Edit a folder’s metadata. Folders `0` and `1` cannot be renamed.
Authentication as the collection owner is required.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to modify. 

folder_id
     `number` (required) **Example:** 3
The ID of the folder to modify.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "id": 392,
  "count": 3,
  "name": "An Example Folder",
  "resource_url": "https://api.discogs.com/users/example/collection/folders/392"
}
```



Delete Folder
DELETE
`/users/{username}/collection/folders/{folder_id}`
Delete a folder from a user’s collection. A folder must be empty before it can be deleted.
Authentication as the collection owner is required.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to modify. 

folder_id
     `number` (required) **Example:** 3
The ID of the folder to delete.


  * **Response`204`**