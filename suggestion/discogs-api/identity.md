## Identity 
Get Identity
GET
`/oauth/identity`
Retrieve basic information about the authenticated user.  
You can use this resource to find out who you’re authenticated as, and it also doubles as a good sanity check to ensure that you’re using OAuth correctly.  
For more detailed information, make another request for the user’s Profile.
  * **Request** Toggle
  * ##### Body
```
GET https://api.discogs.com/oauth/identity
```

  * **Response`200`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 200 OK  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
Content-Encoding: gzip  
Content-Location: https://api.discogs.com/oauth/identity?oauth_body_hash=some_hash&oauth_nonce=...  
Server: lighttpd  
Content-Length: 127  
Date: Tue, 15 Jul 2014 18:44:17 GMT  
X-Varnish: 1718276369  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "id": 1,
  "username": "example",
  "resource_url": "https://api.discogs.com/users/example",
  "consumer_name": "Your Application Name"
}
```

  * **Response`401`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 401 Unauthorized  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
WWW-Authenticate: OAuth realm="https://api.discogs.com"  
Server: lighttpd  
Content-Length: 61  
Date: Wed, 16 Jul 2014 18:42:38 GMT  
X-Varnish: 1718433436  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "message": "You must authenticate to access this resource."
}
```