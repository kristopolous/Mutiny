## Artist 
The Artist resource represents a person in the Discogs database who contributed to a Release in some capacity.
Get Artist
GET
`/artists/{artist_id}`
Get an artist
  * **Parameters**
  * 

artist_id
     `number` (required) **Example:** 108713
The Artist ID


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
  "namevariations": [
    "Nickleback"
  ],
  "profile": "Nickelback is a Canadian rock band from Hanna, Alberta formed in 1995. Nickelback's music is classed as hard rock and alternative metal. Nickelback is one of the most commercially successful Canadian groups, having sold almost 50 million albums worldwide, ranking as the 11th best selling music act of the 2000s, and is the 2nd best selling foreign act in the U.S. behind The Beatles for the 2000's.",
  "releases_url": "https://api.discogs.com/artists/108713/releases",
  "resource_url": "https://api.discogs.com/artists/108713",
  "uri": "https://www.discogs.com/artist/108713-Nickelback",
  "urls": [
    "http://www.nickelback.com/",
    "http://en.wikipedia.org/wiki/Nickelback"
  ],
  "data_quality": "Needs Vote",
  "id": 108713,
  "images": [
    {
      "height": 260,
      "resource_url": "https://api-img.discogs.com/9xJ5T7IBn23DDMpg1USsDJ7IGm4=/330x260/smart/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/A-108713-1110576087.jpg.jpg",
      "type": "primary",
      "uri": "https://api-img.discogs.com/9xJ5T7IBn23DDMpg1USsDJ7IGm4=/330x260/smart/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/A-108713-1110576087.jpg.jpg",
      "uri150": "https://api-img.discogs.com/--xqi8cBtaBZz3qOjVcvzGvNRmU=/150x150/smart/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/A-108713-1110576087.jpg.jpg",
      "width": 330
    },
    {
      "height": 500,
      "resource_url": "https://api-img.discogs.com/r1jRG8b9-nlqTHPlJ-t8JR5ugoA=/493x500/smart/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/A-108713-1264273865.jpeg.jpg",
      "type": "secondary",
      "uri": "https://api-img.discogs.com/r1jRG8b9-nlqTHPlJ-t8JR5ugoA=/493x500/smart/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/A-108713-1264273865.jpeg.jpg",
      "uri150": "https://api-img.discogs.com/6K-cI5xDgsurmc-2OX6uCygzDgw=/150x150/smart/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/A-108713-1264273865.jpeg.jpg",
      "width": 493
    }
  ],
  "members": [
    {
      "active": true,
      "id": 270222,
      "name": "Chad Kroeger",
      "resource_url": "https://api.discogs.com/artists/270222"
    },
    {
      "active": true,
      "id": 685755,
      "name": "Daniel Adair",
      "resource_url": "https://api.discogs.com/artists/685755"
    },
    {
      "active": true,
      "id": 685754,
      "name": "Mike Kroeger",
      "resource_url": "https://api.discogs.com/artists/685754"
    },
    {
      "active": true,
      "id": 685756,
      "name": "Ryan \"Vik\" Vikedal",
      "resource_url": "https://api.discogs.com/artists/685756"
    },
    {
      "active": true,
      "id": 685757,
      "name": "Ryan Peake",
      "resource_url": "https://api.discogs.com/artists/685757"
    }
  ],
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