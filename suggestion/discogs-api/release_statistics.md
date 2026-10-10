## Release Statistics 
Get Marketplace Stats
GET
`/marketplace/stats/{release_id}{?curr_abbr}`
Retrieve marketplace statistics for the provided Release ID. These statistics reflect the state of the release in the marketplace _currently_ , and include the number of items currently for sale, lowest listed price of any item for sale, and whether the item is blocked for sale in the marketplace.
Authentication is optional. Authenticated users will by default have the lowest currency expressed in their own buyer currency, configurable in buyer settings, in the absence of the `curr_abbr` query parameter to specify the currency. Unauthenticated users will have the price expressed in US Dollars, if no `curr_abbr` is provided.
Releases that have no items for sale in the marketplace will return a body with null data in the `lowest_price` and `num_for_sale` keys. Releases that are blocked for sale will also have null data for these keys.
  * **Parameters**
  * 

release_id
     `number` (required) **Example:** 1
The release ID whose stats are desired 

curr_abbr
     `string` (optional) **Example:** USD
Currency for marketplace data. Defaults to the authenticated users currency. Must be one of the following:   
`USD` `GBP` `EUR` `CAD` `AUD` `JPY` `CHF` `MXN` `BRL` `NZD` `SEK` `ZAR`


  * **Response`200`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 200 OK  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 780  
Date: Tue, 15 Jul 2014 19:59:59 GMT  
X-Varnish: 1702965334  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "lowest_price": {
    "currency": "USD",
    "value": 2.09
  },
  "num_for_sale": 26,
  "blocked_from_sale": false
}
```



Next 
* * *
# Inventory Export