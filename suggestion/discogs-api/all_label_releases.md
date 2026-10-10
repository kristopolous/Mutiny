## All Label Releases 
Get all Label Releases
GET
`/labels/{label_id}/releases{?page,per_page}`
Returns a list of Releases associated with the label. Accepts Pagination parameters.
  * **Parameters**
  * 

label_id
     `number` (required) **Example:** 1
The Label ID 

page
     `number` (optional) **Example:** 3
The page you want to request 

per_page
     `number` (optional) **Example:** 25
The number of items per page


  * **Response`200`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 200 OK  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 2834  
Date: Tue, 01 Jul 2014 01:16:01 GMT  
X-Varnish: 1465678820  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "pagination": {
    "per_page": 5,
    "pages": 68,
    "page": 1,
    "urls": {
      "last": "https://api.discogs.com/labels/1/releases?per_page=5&page=68",
      "next": "https://api.discogs.com/labels/1/releases?per_page=5&page=2"
    },
    "items": 338
  },
  "releases": [
    {
      "artist": "Andrea Parker",
      "catno": "!K7071CD",
      "format": "CD, Mixed",
      "id": 2801,
      "resource_url": "http://api.discogs.com/releases/2801",
      "status": "Accepted",
      "thumb": "https://api-img.discogs.com/cYmCut4Yh99FaLFHyoqkFo-Md1E=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/L-1-1111053865.png.jpg",
      "title": "DJ-Kicks",
      "year": 1998
    },
    {
      "artist": "Naomi Daniel",
      "catno": "2INZ 00140",
      "format": "12\"",
      "id": 65040,
      "resource_url": "http://api.discogs.com/releases/65040",
      "status": "Accepted",
      "thumb": "https://api-img.discogs.com/cYmCut4Yh99FaLFHyoqkFo-Md1E=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/L-1-1111053865.png.jpg",
      "title": "Stars",
      "year": 1993
    },
    {
      "artist": "Innerzone Orchestra",
      "catno": "546 137-2",
      "format": "CD, Album, P/Mixed",
      "id": 9922,
      "resource_url": "http://api.discogs.com/releases/9922",
      "status": "Accepted",
      "thumb": "https://api-img.discogs.com/cYmCut4Yh99FaLFHyoqkFo-Md1E=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/L-1-1111053865.png.jpg",
      "title": "Programmed",
      "year": 1999
    }
  ]
}
```

  * **Response`404`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 404 Not Found  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 30  
Date: Tue, 01 Jul 2014 01:17:27 GMT  
X-Varnish: 1465696276  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "message": "Label not found."
}
```