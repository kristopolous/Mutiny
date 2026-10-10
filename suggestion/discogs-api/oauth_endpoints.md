## OAuth Endpoints ¶
### Request Token URL 
Generate the request token
Request token
GET
`/oauth/request_token`
  * **Request** Toggle
  * ##### Headers
```
Content-Type: application/x-www-form-urlencoded  
Authorization: OAuth oauth_consumer_key="your_consumer_key", oauth_nonce="random_string_or_timestamp", oauth_signature="your_consumer_secret&", oauth_signature_method="PLAINTEXT", oauth_timestamp="current_timestamp", oauth_callback="your_callback"  
User-Agent: some_user_agent  

```

  * **Response`200`** Toggle
  * ##### Headers
```
oauth_token: abc123  
oauth_token_secret: xyz789  
oauth_callback_confirmed: 'true'  

```



### Access Token URL 
Generate the access token
Access token
POST
`/oauth/access_token`
  * **Request** Toggle
  * ##### Headers
```
Content-Type: application/x-www-form-urlencoded  
Authorization: OAuth oauth_consumer_key="your_consumer_key", oauth_nonce="random_string_or_timestamp", oauth_token="oauth_token_received_from_step_2" oauth_signature="your_consumer_secret&", oauth_signature_method="PLAINTEXT", oauth_timestamp="current_timestamp", oauth_verifier="users_verifier"  
User-Agent: some_user_agent  

```

  * **Response`200`** Toggle
  * ##### Headers
```
oauth_token: abc123  
oauth_token_secret: xyz789  

```



Next 
* * *
# Database