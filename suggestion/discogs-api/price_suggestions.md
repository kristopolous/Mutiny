## Price Suggestions 
Get Price Suggestions
GET
`/marketplace/price_suggestions/{release_id}`
Retrieve price suggestions for the provided Release ID. If no suggestions are available, an empty object will be returned.  
Authentication is required, and the user needs to have filled out their seller settings. Suggested prices will be denominated in the user’s selling currency.
  * **Parameters**
  * 

release_id
     `number` (required) **Example:** 1
The release ID to calculate a price from.


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
  "Very Good (VG)": {
    "currency": "USD",
    "value": 6.7827501
  },
  "Good Plus (G+)": {
    "currency": "USD",
    "value": 3.7681945000000003
  },
  "Near Mint (NM or M-)": {
    "currency": "USD",
    "value": 12.8118613
  },
  "Good (G)": {
    "currency": "USD",
    "value": 2.2609167
  },
  "Very Good Plus (VG+)": {
    "currency": "USD",
    "value": 9.7973057
  },
  "Mint (M)": {
    "currency": "USD",
    "value": 14.319139100000001
  },
  "Fair (F)": {
    "currency": "USD",
    "value": 1.5072778000000002
  },
  "Poor (P)": {
    "currency": "USD",
    "value": 0.7536389000000001
  }
}
```