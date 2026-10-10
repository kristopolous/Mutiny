## Release Stats 
The Release Stats endpoint retrieves the total number of “haves” (in the community’s collections) and “wants” (in the community’s wantlists) for a given release.
Get Release Stats
GET
`/releases/{release_id}/stats`
Retrieves the release’s “have” and “want” counts.
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
Content-Length: 35  
Date: Mon, 31 Aug 2020 23:22:16 GMT  
X-Varnish: 1465474310  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "num_have": 2315,
  "num_want": 467
}
```