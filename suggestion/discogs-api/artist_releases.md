## Artist Releases 
Returns a list of Releases and Masters associated with the Artist.  
Accepts Pagination.
Get an Artist's Releases
GET
`/artists/{artist_id}/releases{?sort,sort_order}`
Get an artist’s releases
  * **Parameters**
  * 

artist_id
     `number` (required) **Example:** 108713
The Artist ID 

sort
     `string` (optional) **Example:** year
Sort items by this field:   
`year` (i.e. year of the release)  
`title` (i.e. title of the release)  
`format` 

sort_order
     `string` (optional) **Example:** asc
Sort items in a particular order (one of `asc`, `desc`)


  * **Response`200`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 200 OK  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 1258  
Date: Tue, 01 Jul 2014 01:06:53 GMT  
X-Varnish: 1465566651  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "pagination": {
    "per_page": 50,
    "items": 9,
    "page": 1,
    "urls": {},
    "pages": 1
  },
  "releases": [
    {
      "artist": "Nickelback",
      "id": 173765,
      "main_release": 3128432,
      "resource_url": "http://api.discogs.com/masters/173765",
      "role": "Main",
      "thumb": "https://api-img.discogs.com/lb0zp7--FLaRP0LmJ4W6DhfweNc=/fit-in/90x90/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-5557864-1396493975-7618.jpeg.jpg",
      "title": "Curb",
      "type": "master",
      "year": 1996
    },
    {
      "artist": "Nickelback",
      "format": "CD, EP",
      "id": 4299404,
      "label": "Not On Label (Nickelback Self-released)",
      "resource_url": "http://api.discogs.com/releases/4299404",
      "role": "Main",
      "status": "Accepted",
      "thumb": "https://api-img.discogs.com/eFRGD78N7UhtvRjhdLZYXs2QJXk=/fit-in/90x90/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-4299404-1361106117-3632.jpeg.jpg",
      "title": "Hesher",
      "type": "release",
      "year": 1996
    },
    {
      "artist": "Nickelback",
      "id": 173767,
      "main_release": 1905922,
      "resource_url": "http://api.discogs.com/masters/173767",
      "role": "Main",
      "thumb": "https://api-img.discogs.com/12LXbUV44IHjyb6drFZOTQxgCqs=/fit-in/90x90/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1905922-1251540516.jpeg.jpg",
      "title": "Leader Of Men",
      "type": "master",
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
Content-Length: 32  
Date: Tue, 01 Jul 2014 01:08:31 GMT  
X-Varnish: 1465587583  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "message": "Artist not found."
}
```