## User Submissions 
The Submissions resource represents all edits that a user makes to releases, labels, and artist.
Get Submissions
GET
`/users/{username}/submissions`
Retrieve a user’s submissions by username. Accepts Pagination parameters.
  * **Parameters**
  * 

username
     `string` (required) **Example:** shooezgirl
The username of the submissions you are trying to fetch.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "pagination": {
    "items": 3,
    "page": 1,
    "pages": 1,
    "per_page": 50,
    "urls": {}
  },
  "submissions": {
    "artists": [
      {
        "data_quality": "Needs Vote",
        "id": 240177,
        "name": "Grimy",
        "namevariations": [
          "Grimmy"
        ],
        "releases_url": "https://api.discogs.com/artists/240177/releases",
        "resource_url": "https://api.discogs.com/artists/240177",
        "uri": "https://www.discogs.com/artist/240177-Grimy"
      }
    ],
    "labels": [],
    "releases": [
      {
        "artists": [
          {
            "anv": "",
            "id": 17035,
            "join": "",
            "name": "Chaka Khan",
            "resource_url": "https://api.discogs.com/artists/17035",
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
              "resource_url": "https://api.discogs.com/users/Diognes_The_Fox",
              "username": "Diognes_The_Fox"
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
        "date_added": "2014-03-25T14:52:18-07:00",
        "date_changed": "2014-05-14T13:28:21-07:00",
        "estimated_weight": 60,
        "format_quantity": 1,
        "formats": [
          {
            "descriptions": [
              "7\"",
              "45 RPM",
              "Promo"
            ],
            "name": "Vinyl",
            "qty": "1"
          }
        ],
        "genres": [
          "Funk / Soul"
        ],
        "id": 5531861,
        "images": [
          {
            "height": 594,
            "resource_url": "https://api-img.discogs.com/i3jf6S4S7LMNBuWxstCxAQs2Rw0=/fit-in/600x594/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-5531861-1400099290-9223.jpeg.jpg",
            "type": "primary",
            "uri": "https://api-img.discogs.com/i3jf6S4S7LMNBuWxstCxAQs2Rw0=/fit-in/600x594/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-5531861-1400099290-9223.jpeg.jpg",
            "uri150": "https://api-img.discogs.com/xMSwaqP2T8SNwDUTO-gXmHXWt6s=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-5531861-1400099290-9223.jpeg.jpg",
            "width": 600
          },
          {
            "height": 600,
            "resource_url": "https://api-img.discogs.com/sT0plDMoOcfJz-1JEui8XQr69kw=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-5531861-1400099290-9749.jpeg.jpg",
            "type": "secondary",
            "uri": "https://api-img.discogs.com/sT0plDMoOcfJz-1JEui8XQr69kw=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(96)/discogs-images/R-5531861-1400099290-9749.jpeg.jpg",
            "uri150": "https://api-img.discogs.com/f2QfdjBjpuP-Eht4DVSlCfPtTe8=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-5531861-1400099290-9749.jpeg.jpg",
            "width": 600
          }
        ],
        "labels": [
          {
            "catno": "7-28671",
            "entity_type": "1",
            "id": 1000,
            "name": "Warner Bros. Records",
            "resource_url": "https://api.discogs.com/labels/1000"
          }
        ],
        "master_id": 174698,
        "master_url": "https://api.discogs.com/masters/174698",
        "notes": "Promotion Not for Sale",
        "released": "1986",
        "released_formatted": "1986",
        "resource_url": "https://api.discogs.com/releases/5531861",
        "series": [],
        "status": "Accepted",
        "styles": [
          "Rhythm & Blues"
        ],
        "thumb": "https://api-img.discogs.com/xMSwaqP2T8SNwDUTO-gXmHXWt6s=/fit-in/150x150/filters:strip_icc():format(jpeg):mode_rgb()/discogs-images/R-5531861-1400099290-9223.jpeg.jpg",
        "title": "Love Of A Lifetime",
        "uri": "https://www.discogs.com/Chaka-Khan-Love-Of-A-Lifetime/release/5531861",
        "videos": [
          {
            "description": "Chaka Khan - Love you all my lifetime (live)",
            "duration": 285,
            "embed": true,
            "title": "Chaka Khan - Love you all my lifetime (live)",
            "uri": "https://www.youtube.com/watch?v=WOrCYzfchTI"
          },
          {
            "description": "Chaka Khan - Love Of A Lifetime [Official Video]",
            "duration": 257,
            "embed": true,
            "title": "Chaka Khan - Love Of A Lifetime [Official Video]",
            "uri": "https://www.youtube.com/watch?v=5K6-q2VqJdE"
          },
          {
            "description": "CHAKA KHAN - COLTRANE DREAMS  ~{The 45 VERSION}~",
            "duration": 151,
            "embed": true,
            "title": "CHAKA KHAN - COLTRANE DREAMS  ~{The 45 VERSION}~",
            "uri": "https://www.youtube.com/watch?v=11eaG7KdS9g"
          }
        ],
        "year": 1986
      }
    ]
  }
}
```