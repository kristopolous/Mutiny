## User Contributions 
The Contributions resource represents releases, labels, and artists submitted by a user.
Get Contributions
GET
`/users/{username}/contributions{?sort,sort_order}`
Retrieve a user’s contributions by username. Accepts Pagination parameters.
Valid `sort` keys are:  
`label`  
`artist`  
`title`  
`catno`  
`format`  
`rating`  
`year`  
`added`
Valid `sort_order` keys are:  
`asc`  
`desc`
  * **Parameters**
  * 

username
     `string` (required) **Example:** shooezgirl
The username of the submissions you are trying to fetch. 

sort
     `string` (optional) **Example:** artist
Sort items by this field (see below for all valid `sort` keys. 

sort_order
     `string` (optional) **Example:** desc
Sort items in a particular order (`asc` or `desc`)


  * **Response`200`** Toggle
  * ##### Body
```
{
  "contributions": [
    {
      "artists": [
        {
          "anv": "",
          "id": 17961,
          "join": "And",
          "name": "Cher",
          "resource_url": "https://api.discogs.com/artists/17961",
          "role": "",
          "tracks": ""
        }
      ],
      "community": {
        "contributors": [
          {
            "resource_url": "https://api.discogs.com/users/shooezgirl",
            "username": "shooezgirl"
          },
          {
            "resource_url": "https://api.discogs.com/users/Aquacrash_Dj",
            "username": "Aquacrash_Dj"
          }
        ],
        "data_quality": "Needs Vote",
        "have": 0,
        "rating": {
          "average": 0,
          "count": 0
        },
        "status": "Accepted",
        "submitter": {
          "resource_url": "https://api.discogs.com/users/shooezgirl",
          "username": "shooezgirl"
        },
        "want": 0
      },
      "companies": [],
      "country": "US",
      "data_quality": "Needs Vote",
      "date_added": "2014-03-25T15:16:13-07:00",
      "date_changed": "2014-05-14T13:36:00-07:00",
      "estimated_weight": 60,
      "format_quantity": 1,
      "formats": [
        {
          "descriptions": [
            "7\"",
            "45 RPM",
            "Single",
            "Promo"
          ],
          "name": "Vinyl",
          "qty": "1"
        }
      ],
      "genres": [
        "Rock",
        "Pop"
      ],
      "id": 5531933,
      "images": [
        {
          "height": 605,
          "resource_url": "https://api-img.discogs.com/e0X1tNZv6nkdOOTPJAn-dtCbFa0=/fit-in/600x605/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-5531933-1400099758-6444.jpeg.jpg",
          "type": "primary",
          "uri": "https://api-img.discogs.com/e0X1tNZv6nkdOOTPJAn-dtCbFa0=/fit-in/600x605/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-5531933-1400099758-6444.jpeg.jpg",
          "uri150": "https://api-img.discogs.com/8et0xtf9REFloKoqi6NSJ6AJvFI=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-5531933-1400099758-6444.jpeg.jpg",
          "width": 600
        }
      ],
      "labels": [
        {
          "catno": "7-27529-DJ",
          "entity_type": "1",
          "id": 821,
          "name": "Geffen Records",
          "resource_url": "https://api.discogs.com/labels/821"
        }
      ],
      "master_id": 223379,
      "master_url": "https://api.discogs.com/masters/223379",
      "notes": "Promotion Not For Sale",
      "released": "1989",
      "released_formatted": "1989",
      "resource_url": "https://api.discogs.com/releases/5531933",
      "series": [],
      "status": "Accepted",
      "styles": [
        "Ballad"
      ],
      "thumb": "https://api-img.discogs.com/8et0xtf9REFloKoqi6NSJ6AJvFI=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-5531933-1400099758-6444.jpeg.jpg",
      "title": "After All",
      "uri": "https://www.discogs.com/Cher-And-Peter-Cetera-After-All/release/5531933",
      "videos": [
        {
          "description": "Cher & Peter Cetera - After All [On-Screen Lyrics]",
          "duration": 247,
          "embed": true,
          "title": "Cher & Peter Cetera - After All [On-Screen Lyrics]",
          "uri": "https://www.youtube.com/watch?v=qy717J3Iscw"
        }
      ],
      "year": 1989
    }
  ],
  "pagination": {
    "items": 2,
    "page": 1,
    "pages": 1,
    "per_page": 50,
    "urls": {}
  }
}
```



Next 
* * *
# User Collection