## Search 
Issue a search query to our database. This endpoint accepts pagination parameters
Authentication (as any user) is required.
Search
GET
`/database/search?q={query}&{?type,title,release_title,credit,artist,anv,label,genre,style,country,year,format,catno,barcode,track,submitter,contributor}`
Issue a search query
  * **Parameters**
  * 

query
     `string` (optional) **Example:** nirvana
Your search query 

type
     `string` (optional) **Example:** release
String. One of `release`, `master`, `artist`, `label` 

title
     `string` (optional) **Example:** nirvana - nevermind
Search by combined “Artist Name - Release Title” title field. 

release_title
     `string` (optional) **Example:** nevermind
Search release titles. 

credit
     `string` (optional) **Example:** kurt
Search release credits. 

artist
     `string` (optional) **Example:** nirvana
Search artist names. 

anv
     `string` (optional) **Example:** nirvana
Search artist ANV. 

label
     `string` (optional) **Example:** dgc
Search label names. 

genre
     `string` (optional) **Example:** rock
Search genres. 

style
     `string` (optional) **Example:** grunge
Search styles. 

country
     `string` (optional) **Example:** canada
Search release country. 

year
     `string` (optional) **Example:** 1991
Search release year. 

format
     `string` (optional) **Example:** album
Search formats. 

catno
     `string` (optional) **Example:** DGCD-24425
Search catalog number. 

barcode
     `string` (optional) **Example:** 7 2064-24425-2 4
Search barcodes. 

track
     `string` (optional) **Example:** smells like teen spirit
Search track titles. 

submitter
     `string` (optional) **Example:** milKt
Search submitter username. 

contributor
     `string` (optional) **Example:** jerome99
Search contributor usernames.


  * **Request** Toggle
  * ##### Body
```
GET https://api.discogs.com/database/search?release_title=nevermind&artist=nirvana&per_page=3&page=1
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
Server: lighttpd  
Content-Length: 623  
Date: Tue, 15 Jul 2014 18:44:17 GMT  
X-Varnish: 1701844380 1701819611  
Age: 101  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "pagination": {
    "per_page": 3,
    "pages": 66,
    "page": 1,
    "urls": {
      "last": "http://api.discogs.com/database/search?per_page=3&artist=nirvana&release_title=nevermind&page=66",
      "next": "http://api.discogs.com/database/search?per_page=3&artist=nirvana&release_title=nevermind&page=2"
    },
    "items": 198
  },
  "results": [
    {
      "style": [
        "Interview",
        "Grunge"
      ],
      "thumb": "",
      "title": "Nirvana - Nevermind",
      "country": "Australia",
      "format": [
        "DVD",
        "PAL"
      ],
      "uri": "/Nirvana-Nevermind-Classic-Albums/release/2028757",
      "community": {
        "want": 1,
        "have": 5
      },
      "label": [
        "Eagle Vision",
        "Rajon Vision",
        "Classic Albums"
      ],
      "catno": "RV0296",
      "year": "2005",
      "genre": [
        "Non-Music",
        "Rock"
      ],
      "resource_url": "http://api.discogs.com/releases/2028757",
      "type": "release",
      "id": 2028757
    },
    {
      "style": [
        "Interview",
        "Grunge"
      ],
      "thumb": "",
      "title": "Nirvana - Nevermind",
      "country": "France",
      "format": [
        "DVD",
        "PAL"
      ],
      "uri": "/Nirvana-Nevermind-Classic-Albums/release/1852962",
      "community": {
        "want": 4,
        "have": 21
      },
      "label": [
        "Eagle Vision",
        "Classic Albums"
      ],
      "catno": "EV 426200",
      "year": "2005",
      "genre": [
        "Non-Music",
        "Rock"
      ],
      "resource_url": "http://api.discogs.com/releases/1852962",
      "type": "release",
      "id": 1852962
    },
    {
      "style": [
        "Hard Rock",
        "Classic Rock"
      ],
      "thumb": "",
      "format": [
        "UMD"
      ],
      "country": "Europe",
      "barcode": [
        "5 034504 843646"
      ],
      "uri": "/Nirvana-Nevermind/release/3058947",
      "community": {
        "want": 10,
        "have": 3
      },
      "label": [
        "Eagle Vision"
      ],
      "catno": "ERUMD436",
      "genre": [
        "Rock"
      ],
      "title": "Nirvana - Nevermind",
      "resource_url": "http://api.discogs.com/releases/3058947",
      "type": "release",
      "id": 3058947
    }
  ]
}
```

  * **Response`500`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 500 Server Error  
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
  "message": "Query time exceeded. Please try a simpler query."
}
```

  * **Response`500`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 500 Server Error  
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
  "message": "An internal server error occurred. (Malformed query?)"
}
```