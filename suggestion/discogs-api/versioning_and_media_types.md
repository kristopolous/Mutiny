## Versioning and Media Types 
Currently, our API only supports one version: v2. However, you can specify a version in your requests to future-proof your application. By adding an `Accept` header with the version and media type, you can guarantee your requests will receive data from the correct version you develop your app on.
A standard `Accept` header may look like this:  
`application/vnd.discogs.v2.html+json`
If you are requesting information from an endpoint that may have text formatting in it, you can choose which kind of formatting you want to be returned by changing that section of the `Accept` header. We currently support 3 types:  
`application/vnd.discogs.v2.html+json`  
`application/vnd.discogs.v2.plaintext+json`  
`application/vnd.discogs.v2.discogs+json`
If no Accept header is supplied, or if the Accept header differs from one of the three previous options, we default to `application/vnd.discogs.v2.discogs+json`.
* * *