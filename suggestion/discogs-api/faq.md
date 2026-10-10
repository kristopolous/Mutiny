## FAQ 
  1. **Why am I getting an empty response from the server?**
This generally happens when you forget to add a User-Agent header to your requests.
  2. **How do I get updates about the API?**
Subscribe to our API Announcements forum thread. For larger, breaking changes, we will send out an email notice to all developers with a registered Discogs application.
  3. **Where can I register a Discogs application?**
You can register a Discogs application on the Developer Settings.
  4. **If I have a question/issue with the API, should I file a Support Request?**
It's generally best to start out with a forum post on the API topic since other developers may have had similar issues and can point you in the right direction. If the issue requires privacy, then a support request is the best way to go.
  5. **I'm getting a 404 response when trying to fetch images; what gives?**
This may seem obvious, but make sure you aren't doing anything to change the URL. The URLs returned are signed URLs, so trying to change one part of the URL (e.g., Release ID number) will generally not work.
  6. **What are the authentication requirements for requesting images?**
Please see the Images documentation page.
  7. **Why am I getting a particular HTTP response?**
     * **200 OK**
The request was successful, and the requested data is provided in the response body.
     * **201 Continue**
You’ve sent a POST request to a list of resources to create a new one. The ID of the newly-created resource will be provided in the body of the response.
     * **204 No Content**
The request was successful, and the server has no additional information to convey, so the response body is empty.
     * **401 Unauthorized**
You’re attempting to access a resource that first requires authentication. See Authenticating with OAuth.
     * **403 Forbidden**
You’re not allowed to access this resource. Even if you authenticated, or already have, you simply don’t have permission. Trying to modify another user’s profile, for example, will produce this error.
     * **404 Not Found**
The resource you requested doesn’t exist.
     * **405 Method Not Allowed**
You’re trying to use an HTTP verb that isn’t supported by the resource. Trying to PUT to /artists/1, for example, will fail because Artists are read-only.
     * **422 Unprocessable Entity**
Your request was well-formed, but there’s something semantically wrong with the body of the request. This can be due to malformed JSON, a parameter that’s missing or the wrong type, or trying to perform an action that doesn’t make any sense. Check the response body for specific information about what went wrong.
     * **500 Internal Server Error**
Something went wrong on our end while attempting to process your request. The response body’s message field will contain an error code that you can send to Discogs Support (which will help us track down your specific issue).


Next 
* * *
# Authentication 
This section describes the various methods of authenticating with the Discogs API.
* * *