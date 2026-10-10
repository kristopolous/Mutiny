## General Information 
**Your application must provide a User-Agent string that identifies itself** – preferably something that follows RFC 1945. Some good examples include:
```
AwesomeDiscogsBrowser/0.1 +http://adb.example.com
LibraryMetadataEnhancer/0.3 +http://example.com/lime
MyDiscogsClient/1.0 +http://mydiscogsclient.org
```

Please don’t just copy one of those! Make it unique so we can let you know if your application starts to misbehave – the alternative is that we just silently block it, which will confuse and infuriate your users.
Here are some bad examples that are unclear or obscure the nature of the application:
```
curl/7.9.8 (i686-pc-linux-gnu) libcurl 7.9.8 (OpenSSL 0.9.6b)
Mozilla/5.0 (X11; Linux i686; rv:6.0.2) Gecko/20100101 Firefox/6.0.2
my app
```

### JSONP ¶
When a callback query string parameter is supplied, the API can return responses in JSONP format.
JSONP, by its nature, cannot access HTTP information like headers or the status code, so the API supplies that information in the response, like so:
```
GET https://api.discogs.com/artists/1?callback=callbackname
```
```
200 OK
Content-Type: text/javascript
```
```
callbackname({
    "meta": { 
        "status": 200, 
    }, 
    "data": {
        "id": 1,
        "name": "Persuader, The"
        // ... and so on 
    } 
})
```

* * *