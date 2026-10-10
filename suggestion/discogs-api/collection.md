## Collection 
The Collection resource allows you to view and manage a user’s collection.  
A collection is arranged into folders. Every user has two permanent folders already:
ID `0`, the “All” folder, which cannot have releases added to it, and  
ID `1`, the “Uncategorized” folder.
Because it’s possible to own more than one copy of a release, each with its own notes, grading, and so on, each instance of a release in a folder has an instance ID.
Through the Discogs website, users can create custom notes fields. There is not yet an API method for creating and deleting these fields, but they can be listed, and the values of the fields on any instance can be modified.
Get Collection Folders
GET
`/users/{username}/collection/folders`
Retrieve a list of folders in a user’s collection. If the collection has been made private by its owner, authentication as the collection owner is required. If you are not authenticated as the collection owner, only folder ID 0 (the “All” folder) will be visible (if the requested user’s collection is public).
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to retrieve.


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
  "folders": [
    {
      "id": 0,
      "count": 23,
      "name": "All",
      "resource_url": "https://api.discogs.com/users/example/collection/folders/0"
    },
    {
      "id": 1,
      "count": 20,
      "name": "Uncategorized",
      "resource_url": "https://api.discogs.com/users/example/collection/folders/1"
    }
  ]
}
```



Create Folder
POST
`/users/{username}/collection/folders`
Create a new folder in a user’s collection.
Authentication as the collection owner is required.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to retrieve. 

name
     `string` (optional) **Example:** My favorites
The name of the newly-created folder.


  * **Response`201`** Toggle
  * ##### Body
```
{
  "id": 232842,
  "name": "My Music",
  "count": 0,
  "resource_url": "https://api.discogs.com/users/example/collection/folders/232842"
}
```