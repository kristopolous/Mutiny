## Pagination 
Some resources represent collections of objects and may be paginated. By default, 50 items per page are shown.
To browse different pages, or change the number of items per page (up to 100), use the page and per_page query string parameters:
```
GET https://api.discogs.com/artists/1/releases?page=2&per_page=75
```

Responses include a Link header:
```
Link: <https://api.discogs.com/artists/1/releases?page=3&per_page=75>; rel=next,
<https://api.discogs.com/artists/1/releases?page=1&per_page=75>; rel=first,
<https://api.discogs.com/artists/1/releases?page=30&per_page=75>; rel=last,
<https://api.discogs.com/artists/1/releases?page=1&per_page=75>; rel=prev
```

And a pagination object in the response body:
```
{ 
    "pagination": { 
        "page": 2, 
        "pages": 30, 
        "items": 2255, 
        "per_page": 75, 
        "urls":
            { 
                "first": "https://api.discogs.com/artists/1/releases?page=1&per_page=75",
                "prev": "https://api.discogs.com/artists/1/releases?page=1&per_page=75", 
                "next": "https://api.discogs.com/artists/1/releases?page=3&per_page=75", 
                "last": "https://api.discogs.com/artists/1/releases?page=30&per_page=75"
            } 
    }, 
    "releases":
        [ ... ] 
}
```

* * *