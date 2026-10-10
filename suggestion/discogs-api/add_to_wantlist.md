## Add To Wantlist 
Add a release to a user’s wantlist.
Authentication as the wantlist owner is required.
Add To Wantlist
PUT
`/users/{username}/wants/{release_id}{?notes,rating}`
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the wantlist you are trying to fetch. 

release_id
     `number` (required) **Example:** 130076
The ID of the release you are adding. 

notes
     `string` (optional) **Example:** My favorite release
User notes to associate with this release. 

rating
     `number` (optional) **Example:** 5
User’s rating of this release, from `0` (unrated) to `5` (best). Defaults to `0`.


  * **Response`201`** Toggle
  * ##### Body
```
{
  "id": 1,
  "rating": 0,
  "notes": "",
  "resource_url": "https://api.discogs.com/users/example/wants/1",
  "basic_information": {
    "id": 1,
    "resource_url": "https://api.discogs.com/releases/1",
    "thumb": "https://api-img.discogs.com/7HGTQzTb7os1duruukQElELEapk=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1-1193812031.jpeg.jpg",
    "cover_image": "https://api-img.discogs.com/7HGTQzTb7os1duruukQElELEapk=/fit-in/347x352/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1-1193812031.jpeg.jpg",
    "title": "Stockholm",
    "year": 1999,
    "formats": [
      {
        "qty": "2",
        "descriptions": [
          "12\""
        ],
        "name": "Vinyl"
      }
    ],
    "labels": [
      {
        "name": "Svek",
        "entity_type": "1",
        "catno": "SK032",
        "resource_url": "https://api.discogs.com/labels/5",
        "id": 5,
        "entity_type_name": "Label"
      }
    ],
    "artists": [
      {
        "join": "",
        "name": "Persuader, The",
        "anv": "",
        "tracks": "",
        "role": "",
        "resource_url": "https://api.discogs.com/artists/1",
        "id": 1
      }
    ]
  }
}
```



Edit Release In Wantlist
POST
`/users/{username}/wants/{release_id}{?notes,rating}`
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the wantlist you are trying to fetch. 

notes
     `string` (optional) **Example:** My favorite release
User notes to associate with this release. 

rating
     `number` (optional) **Example:** 5
User’s rating of this release, from `0` (unrated) to `5` (best). Defaults to `0`.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "id": 1,
  "rating": 0,
  "notes": "I've added some notes!",
  "resource_url": "https://api.discogs.com/users/example/wants/1",
  "basic_information": {
    "id": 1,
    "resource_url": "https://api.discogs.com/releases/1",
    "thumb": "https://api-img.discogs.com/7HGTQzTb7os1duruukQElELEapk=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1-1193812031.jpeg.jpg",
    "cover_image": "https://api-img.discogs.com/7HGTQzTb7os1duruukQElELEapk=/fit-in/500x500/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1-1193812031.jpeg.jpg",
    "title": "Stockholm",
    "year": 1999,
    "formats": [
      {
        "qty": "2",
        "descriptions": [
          "12\""
        ],
        "name": "Vinyl"
      }
    ],
    "labels": [
      {
        "name": "Svek",
        "entity_type": "1",
        "catno": "SK032",
        "resource_url": "https://api.discogs.com/labels/5",
        "id": 5,
        "entity_type_name": "Label"
      }
    ],
    "artists": [
      {
        "join": "",
        "name": "Persuader, The",
        "anv": "",
        "tracks": "",
        "role": "",
        "resource_url": "https://api.discogs.com/artists/1",
        "id": 1
      }
    ]
  }
}
```



Delete Release From Wantlist
DELETE
`/users/{username}/wants/{release_id}{?notes,rating}`
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the wantlist you are trying to fetch.


  * **Response`204`**


Next 
* * *
# User Lists