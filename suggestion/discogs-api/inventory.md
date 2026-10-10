## Inventory 
Returns the list of listings in a user’s inventory. Accepts Pagination parameters.  
Basic information about each listing and the corresponding release is provided, suitable for display in a list. For detailed information about the release, make another API call to fetch the corresponding Release.
If you are not authenticated as the inventory owner, only items that have a status of For Sale will be visible.  
If you are authenticated as the inventory owner you will get additional `weight`, `format_quantity`, `external_id`, `location`, and `quantity` keys. Note that `quantity` is a read-only field for NearMint users, who will see `1` for all quantity values, regardless of their actual count. If the user is authorized, the listing will contain a in_cart boolean field indicating whether or not this listing is in their cart.
Get inventory
GET
`/users/{username}/inventory{?status,sort,sort_order}`
Get a seller’s inventory
  * **Parameters**
  * 

username
     `string` (required) **Example:** 360vinyl
The username for whose inventory you are fetching 

status
     `string` (optional) **Example:** for sale
Only show items with this status. 

sort
     `string` (optional) **Example:** price
Sort items by this field:   
`listed`  
`price`  
`item` (i.e. the title of the release)  
`artist`  
`label`  
`catno`  
`audio`  
`status` (when authenticated as the inventory owner)  
`location` (when authenticated as the inventory owner) 

sort_order
     `string` (optional) **Example:** asc
Sort items in a particular order (one of `asc`, `desc`)


  * **Response`200`** Toggle
  * Headers```
Status: HTTP/1.1 200 OK  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
Link: <https://api.discogs.com/users/360vinyl/inventory?per_page=50&page=18>; rel="last", <https://api.discogs.com/users/360vinyl/inventory?per_page=50&page=2>; rel="next"  
Server: lighttpd  
Content-Length: 36813  
Date: Tue, 15 Jul 2014 18:53:23 GMT  
X-Varnish: 1701983958  
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
  "listings": [
    {
      "status": "For Sale",
      "price": {
        "currency": "USD",
        "value": 149.99
      },
      "allow_offers": true,
      "sleeve_condition": "Near Mint (NM or M-)",
      "id": 150899904,
      "condition": "Near Mint (NM or M-)",
      "posted": "2014-07-01T10:20:17-07:00",
      "ships_from": "United States",
      "uri": "https://www.discogs.com/sell/item/150899904",
      "comments": "Includes promotional booklet from original purchase!",
      "seller": {
        "username": "rappcats",
        "resource_url": "https://api.discogs.com/users/rappcats",
        "id": 2098225
      },
      "release": {
        "catalog_number": "509990292346, TMR092",
        "resource_url": "https://api.discogs.com/releases/2992668",
        "year": 2011,
        "id": 2992668,
        "description": "Danger Mouse & Daniele Luppi - Rome (LP, Ora + LP, Whi + Album, Ltd, Tip)",
        "artist": "Danger Mouse & Daniele Luppi",
        "title": "Rome",
        "format": "(LP, Ora + LP, Whi + Album, Ltd, Tip)",
        "thumbnail": "https://api-img.discogs.com/CFEw018vfc3LvUQDFtsvkh9FTyA=/fit-in/322x320/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-2992668-1310811542.jpeg.jpg"
      },
      "resource_url": "https://api.discogs.com/marketplace/listings/150899904",
      "audio": false
    },
    {
      "status": "For Sale",
      "price": {
        "currency": "USD",
        "value": 49.99
      },
      "allow_offers": false,
      "sleeve_condition": "Very Good Plus (VG+)",
      "id": 155473349,
      "condition": "Very Good Plus (VG+)",
      "posted": "2014-07-01T10:20:17-07:00",
      "ships_from": "United States",
      "uri": "https://www.discogs.com/sell/item/155473349",
      "comments": "Includes slipmats",
      "seller": {
        "username": "rappcats",
        "resource_url": "https://api.discogs.com/users/rappcats",
        "id": 2098225
      },
      "release": {
        "catalog_number": "STH 2222",
        "resource_url": "https://api.discogs.com/releases/1900152",
        "year": 2009,
        "id": 1900152,
        "description": "Various - Stones Throw X Serato (2x12\", Ltd, Cle)",
        "thumbnail": "https://api-img.discogs.com/BfviIBw5nZOA2BHd0xn8Vfu1X_g=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-1900152-1315429257.jpeg.jpg"
      },
      "resource_url": "https://api.discogs.com/marketplace/listings/155473349",
      "audio": false
    },
    {
      "status": "For Sale",
      "price": {
        "currency": "USD",
        "value": 39.99
      },
      "allow_offers": true,
      "sleeve_condition": "Near Mint (NM or M-)",
      "id": 150899171,
      "condition": "Very Good Plus (VG+)",
      "posted": "2014-07-07T11:40:08-07:00",
      "ships_from": "United States",
      "uri": "https://www.discogs.com/sell/item/150899171",
      "comments": "",
      "seller": {
        "username": "rappcats",
        "resource_url": "https://api.discogs.com/users/rappcats",
        "id": 2098225
      },
      "release": {
        "catalog_number": "STH 2172",
        "resource_url": "https://api.discogs.com/releases/1842118",
        "year": 2009,
        "id": 1842118,
        "description": "Last Electro-Acoustic Space Jazz & Percussion Ensemble, The - Summer Suite (CD, MiniAlbum, Ltd)",
        "thumbnail": "https://api-img.discogs.com/pm6PIqf4vEK8S8rCkySA9eKNFgk=/fit-in/455x455/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-1842118-1247162514.jpeg.jpg"
      },
      "resource_url": "https://api.discogs.com/marketplace/listings/150899171",
      "audio": false
    },
    {
      "status": "For Sale",
      "price": {
        "currency": "USD",
        "value": 229.99
      },
      "allow_offers": false,
      "sleeve_condition": "Near Mint (NM or M-)",
      "id": 171931719,
      "condition": "Near Mint (NM or M-)",
      "posted": "2014-07-12T16:23:14-07:00",
      "ships_from": "United States",
      "uri": "https://www.discogs.com/sell/item/171931719",
      "comments": "Includes poster, includes download card, includes 7\", in original bag w/ hype sticker. Complete set!",
      "seller": {
        "username": "rappcats",
        "resource_url": "https://api.discogs.com/users/rappcats",
        "id": 2098225
      },
      "release": {
        "catalog_number": "NSD-120",
        "resource_url": "https://api.discogs.com/releases/2791275",
        "year": 2011,
        "id": 2791275,
        "description": "Metal Fingers - Presents Special Herbs The Box Set Vol. 0-9 (Box, Comp, Ltd + 10xLP + 7\")",
        "thumbnail": "https://api-img.discogs.com/wyy8_nChnz_ergzK9gd4wxqr-K0=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-2791275-1301188174.jpeg.jpg"
      },
      "resource_url": "https://api.discogs.com/marketplace/listings/171931719",
      "audio": false
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
  "message": "The request resource was not found."
}
```

  * **Response`422`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 422 Unprocessable Entity  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 114  
Date: Tue, 15 Jul 2014 19:16:59 GMT  
X-Varnish: 1702310957  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "message": "Invalid status: expected one of All, Deleted, Draft, Expired, For Sale, Sold, Suspended, Violation."
}
```