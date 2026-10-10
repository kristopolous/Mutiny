## Collection Items By Release 
Get Items By Release
GET
`/users/{username}/collection/releases/{release_id}`
View the user’s collection folders which contain a specified release. This will also show information about each release instance.
The `release_id` must be non-zero.
Authentication as the collection owner is required if the owner’s collection is private.
  * **Parameters**
  * 

username
     `string` (required) **Example:** susan.salkeld
The username of the collection you are trying to view. 

release_id
     `number` (required) **Example:** 7781525
The ID of the release to request.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "pagination": {
    "per_page": 50,
    "items": 28,
    "page": 1,
    "urls": {},
    "pages": 1
  },
  "releases": [
    {
      "instance_id": 148842233,
      "rating": 4,
      "basic_information": {
        "labels": [
          {
            "name": "Varèse Vintage",
            "entity_type": "1",
            "catno": "302 067 361 1",
            "resource_url": "http://api.discogs.com/labels/151979",
            "id": 151979,
            "entity_type_name": "Label"
          }
        ],
        "formats": [
          {
            "descriptions": [
              "LP",
              "Album",
              "Compilation",
              "Limited Edition",
              "Mono"
            ],
            "name": "Vinyl",
            "qty": "2"
          }
        ],
        "thumb": "http://api-img.discogs.com/FGmDbZ6M9wNPwEAsn0yWz1jzQuI=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb():quality(90)/discogs-images/R-7781525-1449187274-5587.jpeg.jpg",
        "title": "The BBC Radio Sessions",
        "artists": [
          {
            "join": ",",
            "name": "The Zombies",
            "anv": "",
            "tracks": "",
            "role": "",
            "resource_url": "http://api.discogs.com/artists/262221",
            "id": 262221
          }
        ],
        "resource_url": "http://api.discogs.com/releases/7781525",
        "year": 2015,
        "id": 7781525
      },
      "folder_id": 688739,
      "date_added": "2015-11-30T10:54:13-08:00",
      "id": 7781525
    },
    {
      "instance_id": 181301430,
      "rating": 4,
      "basic_information": {
        "labels": [
          {
            "name": "Varèse Vintage",
            "entity_type": "1",
            "catno": "302 067 361 1",
            "resource_url": "http://api.discogs.com/labels/151979",
            "id": 151979,
            "entity_type_name": "Label"
          }
        ],
        "formats": [
          {
            "descriptions": [
              "LP",
              "Album",
              "Compilation",
              "Limited Edition",
              "Mono"
            ],
            "name": "Vinyl",
            "qty": "2"
          }
        ],
        "thumb": "http://api-img.discogs.com/FGmDbZ6M9wNPwEAsn0yWz1jzQuI=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb():quality(90)/discogs-images/R-7781525-1449187274-5587.jpeg.jpg",
        "title": "The BBC Radio Sessions",
        "artists": [
          {
            "join": ",",
            "name": "The Zombies",
            "anv": "",
            "tracks": "",
            "role": "",
            "resource_url": "http://api.discogs.com/artists/262221",
            "id": 262221
          }
        ],
        "resource_url": "http://api.discogs.com/releases/7781525",
        "year": 2015,
        "id": 7781525
      },
      "folder_id": 1,
      "date_added": "2016-07-27T08:11:29-07:00",
      "id": 7781525
    },
    {
      "instance_id": 181301442,
      "rating": 4,
      "basic_information": {
        "labels": [
          {
            "name": "Varèse Vintage",
            "entity_type": "1",
            "catno": "302 067 361 1",
            "resource_url": "http://api.discogs.com/labels/151979",
            "id": 151979,
            "entity_type_name": "Label"
          }
        ],
        "formats": [
          {
            "descriptions": [
              "LP",
              "Album",
              "Compilation",
              "Limited Edition",
              "Mono"
            ],
            "name": "Vinyl",
            "qty": "2"
          }
        ],
        "thumb": "http://api-img.discogs.com/FGmDbZ6M9wNPwEAsn0yWz1jzQuI=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb():quality(90)/discogs-images/R-7781525-1449187274-5587.jpeg.jpg",
        "title": "The BBC Radio Sessions",
        "artists": [
          {
            "join": ",",
            "name": "The Zombies",
            "anv": "",
            "tracks": "",
            "role": "",
            "resource_url": "http://api.discogs.com/artists/262221",
            "id": 262221
          }
        ],
        "resource_url": "http://api.discogs.com/releases/7781525",
        "year": 2015,
        "id": 7781525
      },
      "folder_id": 688739,
      "date_added": "2016-07-27T08:11:35-07:00",
      "id": 7781525
    }
  ]
}
```