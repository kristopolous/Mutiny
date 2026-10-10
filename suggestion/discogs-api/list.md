## List 
Get List Items
GET
`/lists/{list_id}`
Returns items from a specified List. Private Lists will only display when authenticated as the owner.
  * **Parameters**
  * 

list_id
     `string` (required) **Example:** 123
The ID of the List you are trying to fetch.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "created_ts": "2016-05-31T10:36:30-07:00",
  "modified_ts": "2016-05-31T13:46:12-07:00",
  "name": "new list",
  "list_id": 2,
  "url": "https://www.discogs.com/lists/new-list/2",
  "items": [
    {
      "comment": "My list comment",
      "display_title": "Silent Phase - The Rewired Mixes",
      "uri": "https://www.discogs.com/Silent-Phase-The-Rewired-Mixes/release/4674",
      "image_url": "https://api-img.discogs.com/-06gF81ykx-Ok1PCpNR7B7Rt_Dc=/300x300/smart/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/A-3227-1132807172.jpeg.jpg",
      "resource_url": "https://api.discogs.com/releases/4674",
      "type": "release",
      "id": 4674
    },
    {
      "comment": "item comment",
      "display_title": "Various - Artificial Intelligence II",
      "uri": "https://www.discogs.com/Various-Artificial-Intelligence-II/release/2964",
      "image_url": "http://api-img.discogs.com/euixsynJwQxJelre_kQNV-ZtX0Y=/fit-in/300x300/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-2964-1215444984.jpeg.jpg",
      "resource_url": "https://api.discogs.com/releases/2964",
      "type": "release",
      "id": 2964
    },
    {
      "comment": "This is an artist",
      "display_title": "Silent Phase",
      "uri": "https://www.discogs.com/artist/3227-Silent-Phase",
      "image_url": "http://api-img.discogs.com/-06gF81ykx-Ok1PCpNR7B7Rt_Dc=/300x300/smart/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/A-3227-1132807172.jpeg.jpg",
      "resource_url": "https://api.discogs.com/artists/3227",
      "type": "artist",
      "id": 3227
    }
  ],
  "resource_url": "https://api.discogs.com/lists/2",
  "public": false,
  "description": "What a cool list!"
}
```




* * *
© 2026 Discogs®
This page may not display correctly when opened as a local file. Instead, view it from a web server.