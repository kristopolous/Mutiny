## OAuth Flow 
OAuth is a protocol that allows users to grant access to their data without having to share their password.
This is an explanation of how a web application may work with Discogs using OAuth 1.0a. We highly suggest you use an OAuth library/wrapper to simplify the process of authenticating.
1. Obtain consumer key and consumer secret from Developer Settings ¶
Application registration can be found here: <https://www.discogs.com/settings/developers>
You only need to register once per application you make. You should not share your consumer secret, as it acts as a sort of password for your requests.
2. Send a GET request to the Discogs request token URL ¶
```
GET https://api.discogs.com/oauth/request_token
```

Include the following headers with your request:
```
Content-Type: application/x-www-form-urlencoded
Authorization:
        OAuth oauth_consumer_key="your_consumer_key",
        oauth_nonce="random_string_or_timestamp",
        oauth_signature="your_consumer_secret&",
        oauth_signature_method="PLAINTEXT",
        oauth_timestamp="current_timestamp",
        oauth_callback="your_callback"
User-Agent: some_user_agent
```

**Note:** using an OAuth library instead of generating these requests manually will likely save you a headache if you are new to OAuth. Please refer to your OAuth library’s documentation if you choose to do so.
As the example shows, we suggest sending requests with HTTPS and the PLAINTEXT signature method over HMAC-SHA1 due to its simple yet secure nature. This involves setting your oauth_signature_method to “PLAINTEXT” and your oauth_signature to be your consumer secret followed by an ampersand (&).
If all goes well with the request, you should get an `HTTP 200 OK` response. If an invalid OAuth request is sent, you will receive an `HTTP 400 Bad Request` response.
Be sure to include a unique User-Agent in the request headers.
A successful request will return a response that contains the following content: OAuth request token (`oauth_token`), an OAuth request token secret (`oauth_token_secret`), and a callback confirmation (`oauth_callback_confirmed`). These will be used in the following steps.
3. Redirect your user to the Discogs Authorize page ¶
Authorization page:
```
https://discogs.com/oauth/authorize?oauth_token=<your_oauth_request_token>
```

This page will ask the user to authorize your app on behalf of their Discogs account. If they accept and authorize, they will receive a verifier key to use as verification. This key is used in the next phase of OAuth authentication.
If you added a callback URL to your Discogs application registration, the user will be redirected to that URL, and you can capture their verifier from the response. The verifier will be used to generate the access token in the next step. You can always edit your application settings to include a callback URL (i.e., you don’t need to re-create a new application).
4. Send a POST request to the Discogs access token URL ¶
```
POST https://api.discogs.com/oauth/access_token
```

Include the following headers in your request:
```
Content-Type: application/x-www-form-urlencoded
Authorization:
        OAuth oauth_consumer_key="your_consumer_key",
        oauth_nonce="random_string_or_timestamp",
        oauth_token="oauth_token_received_from_step_2"
        oauth_signature="your_consumer_secret&",
        oauth_signature_method="PLAINTEXT",
        oauth_timestamp="current_timestamp",
        oauth_verifier="users_verifier"
User-Agent: some_user_agent
```

If the OAuth access token is not created within 15 minutes of when you receive the OAuth request token, your OAuth request token and verifier will expire, and you will need to re-create them. If you try to POST to the access token URL with an expired verifier or your request is malformed, you will receive an HTTP 400 Bad Request response.
As the example shows, we suggest sending requests with HTTPS and the PLAINTEXT signature method over HMAC-SHA1 due to its simple yet secure nature. This involves setting your oauth_signature_method to “PLAINTEXT” and your oauth_signature to be your consumer secret followed by an ampersand (&).
Be sure to include a unique User-Agent in the header.
A successful request will return a response that contains an OAuth access token (`oauth_token`) and an OAuth access token secret (`oauth_token_secret`). These tokens do not expire (unless the user revokes access from your app), so you should store these tokens in a database or persistent storage to make future requests signed with OAuth. All requests that require OAuth will require these two tokens to be sent in the request.
5. Send authenticated requests to Discogs endpoints ¶
You are now ready to send authenticated requests with Discogs through OAuth. Be sure to attach the user’s OAuth access token and OAuth access token secret to each request.
To test that you are ready to send authenticated requests, send a GET request to the identity URL. A successful request will yield a response that contains information about the authenticated user.
```
GET https://api.discogs.com/oauth/identity
```

* * *