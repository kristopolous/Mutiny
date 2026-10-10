## Wantlist 
The Wantlist resource allows you to view and manage a user’s wantlist.
Get Wantlist
GET
`/users/{username}/wants`
Returns the list of releases in a user’s wantlist. Accepts Pagination parameters.
Basic information about each release is provided, suitable for display in a list. For detailed information, make another API call to fetch the corresponding release.
If the wantlist has been made private by its owner, you must be authenticated as the owner to view it.
The `notes` field will be visible if you are authenticated as the wantlist owner.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the wantlist you are trying to fetch.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "pagination": {
    "per_page": 50,
    "pages": 1,
    "page": 1,
    "items": 2,
    "urls": {}
  },
  "wants": [
    {
      "rating": 4,
      "basic_information": {
        "formats": [
          {
            "text": "Digipak",
            "qty": "1",
            "descriptions": [
              "Album"
            ],
            "name": "CD"
          }
        ],
        "thumb": "https://api-img.discogs.com/PsLAcp_I0-EPPkSBaHx2t7dmXTg=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1867708-1248886216.jpeg.jpg",
        "cover_image": "https://api-img.discogs.com/PsLAcp_I0-EPPkSBaHx2t7dmXTg=/fit-in/500x500/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1867708-1248886216.jpeg.jpg",
        "title": "Year Zero",
        "labels": [
          {
            "resource_url": "https://api.discogs.com/labels/2311",
            "entity_type": "",
            "catno": "B0008764-02",
            "id": 2311,
            "name": "Interscope Records"
          }
        ],
        "year": 2007,
        "artists": [
          {
            "join": "",
            "name": "Nine Inch Nails",
            "anv": "",
            "tracks": "",
            "role": "",
            "resource_url": "https://api.discogs.com/artists/3857",
            "id": 3857
          }
        ],
        "resource_url": "https://api.discogs.com/releases/1867708",
        "id": 1867708
      },
      "resource_url": "https://api.discogs.com/users/example/wants/1867708",
      "id": 1867708
    },
    {
      "rating": 0,
      "basic_information": {
        "formats": [
          {
            "qty": "1",
            "descriptions": [
              "Album"
            ],
            "name": "CDr"
          }
        ],
        "thumb": "https://api-img.discogs.com/w1cVy7ppMYEDlqY9sjoAojC3MhQ=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1675174-1236118359.jpeg.jpg",
        "cover_image": "https://api-img.discogs.com/w1cVy7ppMYEDlqY9sjoAojC3MhQ=/fit-in/347x352/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-1675174-1236118359.jpeg.jpg",
        "title": "Dawn Metropolis",
        "labels": [
          {
            "resource_url": "https://api.discogs.com/labels/141550",
            "entity_type": "",
            "catno": "NORM007",
            "id": 141550,
            "name": "Normative"
          }
        ],
        "year": 2009,
        "artists": [
          {
            "join": "",
            "name": "Anamanaguchi",
            "anv": "",
            "tracks": "",
            "role": "",
            "resource_url": "https://api.discogs.com/artists/667233",
            "id": 667233
          }
        ],
        "resource_url": "https://api.discogs.com/releases/1675174",
        "id": 1675174
      },
      "notes": "Sample notes.",
      "resource_url": "https://api.discogs.com/users/example/wants/1675174",
      "id": 1675174
    }
  ]
}
```