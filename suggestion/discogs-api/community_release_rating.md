## Community Release Rating 
The Community Release Rating endpoint retrieves the average rating and the total number of user ratings for a given release.
Get Community Release Rating
GET
`/releases/{release_id}/rating`
Retrieves the community release rating average and count.
  * **Parameters**
  * 

release_id
     `number` (required) **Example:** 249504
The Release ID


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
    "rating": {
        "count": 47
        "average": 4.19
    },
    "release_id": 249504
}
```