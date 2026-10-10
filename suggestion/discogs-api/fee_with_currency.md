## Fee with currency 
Calculate Fee
GET
`/marketplace/fee/{price}/{currency}`
The Fee resource allows you to quickly calculate the fee for selling an item on the Marketplace given a particular currency.
  * **Parameters**
  * 

price
     `number` (optional) **Example:** 10.00
The price to calculate a fee from 

currency
     `string` (optional) **Example:** USD
Defaults to `USD`. Must be one of the following:   
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
    "value": 0.42,
    "currency": "USD",
}
```