## User Lists 
The List resource allows you to view a User’s Lists.
Get Lists
GET
`/users/{username}/lists`
Returns a User’s Lists. Private Lists will only display when authenticated as the owner. Accepts Pagination parameters.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the Lists you are trying to fetch.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "pagination": {
    "per_page": 1,
    "items": 2,
    "page": 2,
    "urls": {
      "prev": "https://api.discogs.com/users/rodneyfool/lists?per_page=1&page=1",
      "first": "https://api.discogs.com/users/rodneyfool/lists?per_page=1&page=1"
    },
    "pages": 2
  },
  "lists": [
    {
      "date_added": "2016-05-31T10:30:51-07:00",
      "date_changed": "2016-05-31T10:30:51-07:00",
      "name": "rodneyfool",
      "id": 1,
      "uri": "https://www.discogs.com/lists/rodneyfool/1",
      "resource_url": "https://api.discogs.com/lists/1",
      "public": false,
      "description": "foo test description"
    }
  ]
}
```