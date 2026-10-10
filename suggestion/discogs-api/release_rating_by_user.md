## Release Rating By User 
The Release Rating endpoint retrieves, updates, or deletes the rating of a release for a given user.
Get Release Rating By User
GET
`/releases/{release_id}/rating/{username}`
Retrieves the release’s rating for a given user.
  * **Parameters**
  * 

release_id
     `number` (required) **Example:** 249504
The Release ID 

username
     `string` (required) **Example:** memory
The username of the rating you are trying to request.


  * **Response`200`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 200 OK  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 6223  
Date: Tue, 01 Jul 2014 00:59:34 GMT  
X-Varnish: 1465474310  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "username": "memory",
  "release_id": 249504,
  "rating": 5
}
```



Update Release Rating By User
PUT
`/releases/{release_id}/rating/{username}`
Updates the release’s rating for a given user. Authentication as the user is required.
  * **Parameters**
  * 

release_id
     `number` (required) **Example:** 249504
The Release ID 

username
     `string` (required) **Example:** memory
The username of the rating you are trying to request. 

rating
     `int` (required) **Example:** 5
The new rating for a release between 1 and 5.


  * **Response`200`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 200 OK  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 6223  
Date: Tue, 01 Jul 2014 00:59:34 GMT  
X-Varnish: 1465474310  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "username": "memory",
  "release_id": 249504,
  "rating": 5
}
```



Delete Release Rating By User
DELETE
`/releases/{release_id}/rating/{username}`
Deletes the release’s rating for a given user. Authentication as the user is required.
  * **Parameters**
  * 

release_id
     `number` (required) **Example:** 249504
The Release ID 

username
     `string` (required) **Example:** memory
The username of the rating you are trying to request.