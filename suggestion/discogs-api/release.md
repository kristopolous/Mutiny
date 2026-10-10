## Release 
The Release resource represents a particular physical or digital object released by one or more Artists.
Get Release
GET
`/releases/{release_id}{?curr_abbr}`
Get a release
  * **Parameters**
  * 

release_id
     `number` (required) **Example:** 249504
The Release ID 

curr_abbr
     `string` (optional) **Example:** USD
Currency for marketplace data. Defaults to the authenticated users currency. Must be one of the following:   
`USD` `GBP` `EUR` `CAD` `AUD` `JPY` `CHF` `MXN` `BRL` `NZD` `SEK` `ZAR`


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
    "title": "Never Gonna Give You Up",
    "id": 249504,
    "artists": [
        {
            "anv": "",
            "id": 72872,
            "join": "",
            "name": "Rick Astley",
            "resource_url": "https://api.discogs.com/artists/72872",
            "role": "",
            "tracks": ""
        }
    ],
    "data_quality": "Correct",
    "thumb": "https://api-img.discogs.com/kAXVhuZuh_uat5NNr50zMjN7lho=/fit-in/300x300/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-249504-1334592212.jpeg.jpg",
    "community": {
        "contributors": [
            {
                "resource_url": "https://api.discogs.com/users/memory",
                "username": "memory"
            },
            {
                "resource_url": "https://api.discogs.com/users/_80_",
                "username": "_80_"
            }
        ],
        "data_quality": "Correct",
        "have": 252,
        "rating": {
            "average": 3.42,
            "count": 45
        },
        "status": "Accepted",
        "submitter": {
            "resource_url": "https://api.discogs.com/users/memory",
            "username": "memory"
        },
        "want": 42
    },
    "companies": [
        {
            "catno": "",
            "entity_type": "13",
            "entity_type_name": "Phonographic Copyright (p)",
            "id": 82835,
            "name": "BMG Records (UK) Ltd.",
            "resource_url": "https://api.discogs.com/labels/82835"
        },
        {
            "catno": "",
            "entity_type": "29",
            "entity_type_name": "Mastered At",
            "id": 266218,
            "name": "Utopia Studios",
            "resource_url": "https://api.discogs.com/labels/266218"
        }
    ],
    "country": "UK",
    "date_added": "2004-04-30T08:10:05-07:00",
    "date_changed": "2012-12-03T02:50:12-07:00",
    "estimated_weight": 60,
    "extraartists": [
        {
            "anv": "Me Co",
            "id": 547352,
            "join": "",
            "name": "Me Company",
            "resource_url": "https://api.discogs.com/artists/547352",
            "role": "Design",
            "tracks": ""
        },
        {
            "anv": "Stock / Aitken / Waterman",
            "id": 20942,
            "join": "",
            "name": "Stock, Aitken & Waterman",
            "resource_url": "https://api.discogs.com/artists/20942",
            "role": "Producer, Written-By",
            "tracks": ""
        }
    ],
    "format_quantity": 1,
    "formats": [
        {
            "descriptions": [
                "7\"",
                "Single",
                "45 RPM"
            ],
            "name": "Vinyl",
            "qty": "1"
        }
    ],
    "genres": [
        "Electronic",
        "Pop"
    ],
    "identifiers": [
        {
            "type": "Barcode",
            "value": "5012394144777"
        },
    ],
    "images": [
        {
            "height": 600,
            "resource_url": "https://api-img.discogs.com/z_u8yqxvDcwVnR4tX2HLNLaQO2Y=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-249504-1334592212.jpeg.jpg",
            "type": "primary",
            "uri": "https://api-img.discogs.com/z_u8yqxvDcwVnR4tX2HLNLaQO2Y=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-249504-1334592212.jpeg.jpg",
            "uri150": "https://api-img.discogs.com/0ZYgPR4X2HdUKA_jkhPJF4SN5mM=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-249504-1334592212.jpeg.jpg",
            "width": 600
        },
        {
            "height": 600,
            "resource_url": "https://api-img.discogs.com/EnQXaDOs5T6YI9zq-R5I_mT7hSk=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-249504-1334592228.jpeg.jpg",
            "type": "secondary",
            "uri": "https://api-img.discogs.com/EnQXaDOs5T6YI9zq-R5I_mT7hSk=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-249504-1334592228.jpeg.jpg",
            "uri150": "https://api-img.discogs.com/abk0FWgWsRDjU4bkCDwk0gyMKBo=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-249504-1334592228.jpeg.jpg",
            "width": 600
        }
    ],
    "labels": [
        {
            "catno": "PB 41447",
            "entity_type": "1",
            "id": 895,
            "name": "RCA",
            "resource_url": "https://api.discogs.com/labels/895"
        }
    ],
    "lowest_price": 0.63,
    "master_id": 96559,
    "master_url": "https://api.discogs.com/masters/96559",
    "notes": "UK Release has a black label with the text \"Manufactured In England\" printed on it.\r\n\r\nSleeve:\r\n\u2117 1987 \u2022 BMG Records (UK) Ltd. \u00a9 1987 \u2022 BMG Records (UK) Ltd.\r\nDistributed in the UK by BMG Records \u2022  Distribu\u00e9 en Europe par BMG/Ariola \u2022 Vertrieb en Europa d\u00fcrch BMG/Ariola.\r\n\r\nCenter labels:\r\n\u2117 1987 Pete Waterman Ltd.\r\nOriginal Sound Recording made by PWL.\r\nBMG Records (UK) Ltd. are the exclusive licensees for the world.\r\n\r\nDurations do not appear on the release.\r\n",
    "num_for_sale": 58,
    "released": "1987",
    "released_formatted": "1987",
    "resource_url": "https://api.discogs.com/releases/249504",
    "series": [],
    "status": "Accepted",
    "styles": [
        "Synth-pop"
    ],
    "tracklist": [
        {
            "duration": "3:32",
            "position": "A",
            "title": "Never Gonna Give You Up",
            "type_": "track"
        },
        {
            "duration": "3:30",
            "position": "B",
            "title": "Never Gonna Give You Up (Instrumental)",
            "type_": "track"
        }
    ],
    "uri": "https://www.discogs.com/Rick-Astley-Never-Gonna-Give-You-Up/release/249504",
    "videos": [
        {
            "description": "Rick Astley - Never Gonna Give You Up (Extended Version)",
            "duration": 330,
            "embed": true,
            "title": "Rick Astley - Never Gonna Give You Up (Extended Version)",
            "uri": "https://www.youtube.com/watch?v=te2jJncBVG4"
        },
    ],
    "year": 1987
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
Content-Length: 33  
Date: Tue, 01 Jul 2014 01:03:20 GMT  
X-Varnish: 1465521729  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "message": "Release not found."
}
```