## Profile 
Get Profile
GET
`/users/{username}`
Retrieve a user by username.  
If authenticated as the requested user, the `email` key will be visible, and the `num_list` count will include the user’s private lists.  
If authenticated as the requested user or the user’s collection/wantlist is public, the `num_collection` / `num_wantlist` keys will be visible.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of whose profile you are requesting.


  * **Response`200`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 200 OK  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 858  
Date: Wed, 16 Jul 2014 18:46:21 GMT  
X-Varnish: 1718492795  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "profile": "I am a software developer for Discogs.\r\n\r\n[img=http://i.imgur.com/IAk3Ukk.gif]",
  "wantlist_url": "https://api.discogs.com/users/rodneyfool/wants",
  "rank": 149,
  "num_pending": 61,
  "id": 1578108,
  "num_for_sale": 0,
  "home_page": "",
  "location": "I live in the good ol' Pacific NW",
  "collection_folders_url": "https://api.discogs.com/users/rodneyfool/collection/folders",
  "username": "rodneyfool",
  "collection_fields_url": "https://api.discogs.com/users/rodneyfool/collection/fields",
  "releases_contributed": 5,
  "registered": "2012-08-15T21:13:36-07:00",
  "rating_avg": 3.47,
  "num_collection": 78,
  "releases_rated": 116,
  "num_lists": 0,
  "name": "Rodney",
  "num_wantlist": 160,
  "inventory_url": "https://api.discogs.com/users/rodneyfool/inventory",
  "avatar_url": "http://www.gravatar.com/avatar/55502f40dc8b7c769880b10874abc9d0?s=52&r=pg&d=mm",
  "banner_url": "https://img.discogs.com/dhuJe-pRJmod7hN3cdVi2PugEh4=/1600x400/filters:strip_icc():format(jpeg)/discogs-banners/B-1578108-user-1436314164-9231.jpg.jpg",
  "uri": "https://www.discogs.com/user/rodneyfool",
  "resource_url": "https://api.discogs.com/users/rodneyfool",
  "buyer_rating": 100.00,
  "buyer_rating_stars": 5,
  "buyer_num_ratings": 144,
  "seller_rating": 100.00,
  "seller_rating_stars": 5,
  "seller_num_ratings": 21,
  "curr_abbr": "USD",
}
```



Edit Profile
POST
`/users/{username}`
Edit a user’s profile data.
Authentication as the user is required.
  * **Parameters**
  * 

username
     `string` (required) **Example:** vreon
The username of the user. 

name
     `string` (optional) **Example:** Nicolas Cage
The real name of the user. 

home_page
     `string` (optional) **Example:** www.discogs.com
The user’s website. 

location
     `string` (optional) **Example:** Portland
The geographical location of the user. 

profile
     `string` (optional) **Example:** I am a Discogs user!
Biographical information about the user. 

curr_abbr
     `string` (optional) **Example:** USD
Currency for marketplace data. Must be one of the following:   
`USD` `GBP` `EUR` `CAD` `AUD` `JPY` `CHF` `MXN` `BRL` `NZD` `SEK` `ZAR`


  * **Response`200`** Toggle
  * ##### Body
```
{
  "id": 1,
  "username": "example",
  "name": "Example Sampleman",
  "email": "sampleman@example.com",
  "resource_url": "https://api.discogs.com/users/example",
  "inventory_url": "https://api.discogs.com/users/example/inventory",
  "collection_folders_url": "https://api.discogs.com/users/example/collection/folders",
  "collection_fields_url": "https://api.discogs.com/users/example/collection/fields",
  "wantlist_url": "https://api.discogs.com/users/example/wants",
  "uri": "https://www.discogs.com/user/example",
  "profile": "This is some [b]different sample[/b] data!",
  "home_page": "http://www.example.com",
  "location": "Australia",
  "registered": "2011-08-30 14:21:45-07:00",
  "num_lists": 0,
  "num_for_sale": 6,
  "num_collection": 4,
  "num_wantlist": 5,
  "num_pending": 10,
  "releases_contributed": 15,
  "rank": 30,
  "releases_rated": 4,
  "rating_avg": 2.5
}
```

  * **Response`403`** Toggle
  * ##### Body
```
{
  "message": "You don't have permission to access this resource."
}
```