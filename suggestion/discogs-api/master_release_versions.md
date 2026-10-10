## Master Release Versions 
Get all Master versions
GET
`/masters/{master_id}/versions{?page,per_page}`
Retrieves a list of all Releases that are versions of this master. Accepts Pagination parameters.
  * **Parameters**
  * 

master_id
     `number` (required) **Example:** 1000
The Master ID 

page
     `number` (optional) **Example:** 3
The page you want to request 

per_page
     `number` (optional) **Example:** 25
The number of items per page 

format
     `string` (optional) **Example:** Vinyl
The format to filter 

label
     `string` (optional) **Example:** Scorpio Music
The label to filter 

released
     `string` (optional) **Example:** 1992
The release year to filter 

country
     `string` (optional) **Example:** Belgium
The country to filter 

sort
     `string` (optional) **Example:** released
Sort items by this field:   
`released` (i.e. year of the release)  
`title` (i.e. title of the release)  
`format`  
`label`  
`catno` (i.e. catalog number of the release)  
`country` 

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
    "per_page": 50,
    "items": 4,
    "page": 1,
    "urls": {},
    "pages": 1
  },
  "versions": [
    {
      "status": "Accepted",
      "stats": {
        "user": {
          "in_collection": 0,
          "in_wantlist": 0
        },
        "community": {
          "in_collection": 1067,
          "in_wantlist": 765
        }
      },
      "thumb": "https://img.discogs.com/wV56xo0Ak0M2bTCC6B_heD7Dx_o=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-18926-1381225536-8716.jpeg.jpg",
      "format": "12\", 33 ⅓ RPM",
      "country": "US",
      "title": "Plastic Dreams",
      "label": "Epic",
      "released": "1993",
      "major_formats": [
        "Vinyl"
      ],
      "catno": "49 74992",
      "resource_url": "https://api.discogs.com/releases/18926",
      "id": 18926
    },
    {
      "status": "Accepted",
      "stats": {
        "user": {
          "in_collection": 0,
          "in_wantlist": 0
        },
        "community": {
          "in_collection": 842,
          "in_wantlist": 762
        }
      },
      "thumb": "https://img.discogs.com/KAm38-Op5VlkvuJOaTGZieKwRVg=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-34228-1215760312.jpeg.jpg",
      "format": "12\", 33 ⅓ RPM",
      "country": "UK",
      "title": "Plastic Dreams (Mixes)",
      "label": "R & S Records",
      "released": "1993",
      "major_formats": [
        "Vinyl"
      ],
      "catno": "RSGB 101T",
      "resource_url": "https://api.discogs.com/releases/34228",
      "id": 34228
    },
    {
      "status": "Accepted",
      "stats": {
        "user": {
          "in_collection": 0,
          "in_wantlist": 0
        },
        "community": {
          "in_collection": 20,
          "in_wantlist": 285
        }
      },
      "thumb": "https://img.discogs.com/CnovcvhAYIle0tYTTZHo42wPz88=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-1873038-1249267784.jpeg.jpg",
      "format": "12\", White Label, 33 ⅓ RPM, Promo",
      "country": "UK",
      "title": "Plastic Dreams Mixes",
      "label": "R & S Records",
      "released": "1993",
      "major_formats": [
        "Vinyl"
      ],
      "catno": "RSGB 101 T",
      "resource_url": "https://api.discogs.com/releases/1873038",
      "id": 1873038
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
Content-Length: 40  
Date: Tue, 01 Jul 2014 01:20:23 GMT  
X-Varnish: 1465732620  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "message": "Master Release not found."
}
```