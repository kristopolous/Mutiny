## Rate Limiting 
**Requests are throttled by the server by source IP to 60 per minute for authenticated requests, and 25 per minute for unauthenticated requests, with some exceptions.**
Your application should identify itself to our servers via a unique user agent string in order to achieve the maximum number of requests per minute.
Our rate limiting tracks your requests using a moving average over a 60 second window. If no requests are made in 60 seconds, your window will reset.
We attach the following headers to responses to help you track your rate limit use:
`X-Discogs-Ratelimit`: The total number of requests you can make in a one minute window.
`X-Discogs-Ratelimit-Used` : The number of requests you’ve made in your existing rate limit window.
`X-Discogs-Ratelimit-Remaining`: The number of remaining requests you are able to make in the existing rate limit window.
Your application should take our global limit into account and throttle its requests locally.
In the future, we may update these rate limits at any time in order to provide service for all users.
* * *