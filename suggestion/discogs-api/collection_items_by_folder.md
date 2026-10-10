## Collection Items By Folder 
Get Items
GET
`/users/{username}/collection/folders/{folder_id}/releases`
Returns the list of item in a folder in a user’s collection. Accepts Pagination parameters.
Basic information about each release is provided, suitable for display in a list. For detailed information, make another API call to fetch the corresponding release.
If `folder_id` is not `0`, or the collection has been made private by its owner, authentication as the collection owner is required.
If you are not authenticated as the collection owner, only public notes fields will be visible.
Valid `sort` keys are:  
`label`  
`artist`  
`title`  
`catno`  
`format`  
`rating`  
`added`  
`year`
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to request. 

folder_id
     `number` (required) **Example:** 3
The ID of the folder to request. 

sort
     `string` (optional) **Example:** artist
Sort items by this field (see below for all valid `sort` keys. 

sort_order
     `string` (optional) **Example:** desc
Sort items in a particular order (`asc` or `desc`)


  * **Response`200`** Toggle
  * ##### Headers
```
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 132  
Date: Wed, 16 Jul 2014 23:20:21 GMT  
X-Varnish: 1722533701  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "pagination": {
    "per_page": 1,
    "pages": 14,
    "page": 1,
    "items": 14,
    "urls": {
      "next": "https://api.discogs.com/users/example/collection/folders/1/releases?page=2&per_page=1",
      "last": "https://api.discogs.com/users/example/collection/folders/1/releases?page=2&per_page=14",
    }
  },
  "releases": [
    {
      "id": 2464521,
      "instance_id": 1,
      "folder_id": 1,
      "rating": 0,
      "basic_information": {
        "id": 2464521,
        "title": "Information Chase",
        "year": 2006,
        "resource_url": "https://api.discogs.com/releases/2464521",
        "thumb": "https://api-img.discogs.com/vzpYq4_kc52GZFs14c0SCJ0ZE84=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-2464521-1285519861.jpeg.jpg",
        "cover_image": "https://api-img.discogs.com/vzpYq4_kc52GZFs14c0SCJ0ZE84/fit-in/500x500/filters:strip_icc():format(jpeg):mode_rgb():quality(90)/discogs-images/R-2464521-1285519861.jpeg.jpg",
        "formats": [
          {
            "qty": "1",
            "descriptions": [ "Mini", "EP" ],
            "name": "CDr"
          }
        ],
        "labels": [
          {
            "resource_url": "https://api.discogs.com/labels/11647",
            "entity_type": "",
            "catno": "8BP059",
            "id": 11647,
            "name": "8bitpeoples"
          }
        ],
        "artists": [
          {
            "id": 103906,
            "name": "Bit Shifter",
            "join": "",
            "resource_url": "https://api.discogs.com/artists/103906",
            "anv": "",
            "tracks": "",
            "role": ""
          }
        ],
        "genres": [
            "Electronic", 
            "Pop"
        ],
        "styles": [
            "Chiptune"
        ]
      },
      "notes": [
        {
          "field_id": 3,
          "value": "bleep bloop blorp."
        }
      ]
    }
  ]
}
```