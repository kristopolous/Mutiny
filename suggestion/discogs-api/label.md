## Label 
The Label resource represents a label, company, recording studio, location, or other entity involved with Artists and Releases. Labels were recently expanded in scope to include things that aren’t labels – the name is an artifact of this history.
Get Label
GET
`/labels/{label_id}`
Get a label
  * **Parameters**
  * 

label_id
     `number` (required) **Example:** 1
The Label ID


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
  "profile": "Classic Techno label from Detroit, USA.\r\n[b]Label owner:[/b] [a=Carl Craig].\r\n",
  "releases_url": "https://api.discogs.com/labels/1/releases",
  "name": "Planet E",
  "contact_info": "Planet E Communications\r\nP.O. Box 27218\r\nDetroit, 48227, USA\r\n\r\np: 313.874.8729 \r\nf: 313.874.8732\r\n\r\nemail: info AT Planet-e DOT net\r\n",
  "uri": "https://www.discogs.com/label/1-Planet-E",
  "sublabels": [
    {
      "resource_url": "https://api.discogs.com/labels/86537",
      "id": 86537,
      "name": "Antidote (4)"
    },
    {
      "resource_url": "https://api.discogs.com/labels/41841",
      "id": 41841,
      "name": "Community Projects"
    }
  ],
  "urls": [
    "http://www.planet-e.net",
    "http://planetecommunications.bandcamp.com",
    "http://twitter.com/planetedetroit"
  ],
  "images": [
    {
      "height": 24,
      "resource_url": "https://api-img.discogs.com/85-gKw4oEXfDp9iHtqtCF5Y_ZgI=/fit-in/132x24/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/L-1-1111053865.png.jpg",
      "type": "primary",
      "uri": "https://api-img.discogs.com/85-gKw4oEXfDp9iHtqtCF5Y_ZgI=/fit-in/132x24/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/L-1-1111053865.png.jpg",
      "uri150": "https://api-img.discogs.com/cYmCut4Yh99FaLFHyoqkFo-Md1E=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/L-1-1111053865.png.jpg",
      "width": 132
    }
  ],
  "resource_url": "https://api.discogs.com/labels/1",
  "id": 1,
  "data_quality": "Needs Vote"
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