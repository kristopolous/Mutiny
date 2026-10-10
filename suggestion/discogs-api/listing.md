## Listing 
View the data associated with a listing.  
If the authorized user is the listing owner the listing will include the `weight`, `format_quantity`, `external_id`, `location`, and `quantity` keys. Note that `quantity` is a read-only field for NearMint users, who will see `1` for all quantity values, regardless of their actual count. If the user is authorized, the listing will contain a in_cart boolean field indicating whether or not this listing is in their cart.
Get listing
GET
`/marketplace/listings/{listing_id}{?curr_abbr}`
The Listing resource allows you to view Marketplace listings.
  * **Parameters**
  * 

listing_id
     `number` (required) **Example:** 172723812
The ID of the listing you are fetching 

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
  "status": "For Sale",
  "price": {
    "currency": "USD",
    "value": 120
  },
  "original_price": {
    "curr_abbr": "USD",
    "curr_id": 1,
    "formatted": "$120.00",
    "value": 120.0
  },
  "allow_offers": false,
  "sleeve_condition": "Mint (M)",
  "id": 172723812,
  "condition": "Mint (M)",
  "posted": "2014-07-15T12:55:01-07:00",
  "ships_from": "United States",
  "uri": "https://www.discogs.com/sell/item/172723812",
  "comments": "Brand new... Still sealed!",
  "seller": {
    "username": "Booms528",
    "avatar_url": "https://secure.gravatar.com/avatar/8aa676fcfa2be14266d0ccad88da2cc4?s=500&r=pg&d=mm",
    "resource_url": "https://api.discogs.com/users/Booms528",
    "url": "https://api.discogs.com/users/Booms528",
    "id": 1369620
    "shipping": "Buyer responsible for shipping. Price depends on distance but is usually $5-10.",
    "payment": "PayPal",
    "stats": {
      "rating": "100",
      "stars": 5.0,
      "total": 15
    }
  },
  "shipping_price": {
    "currency": "USD",
    "value": 2.50
  },
  "original_shipping_price": {
    "curr_abbr": "USD",
    "curr_id": 1,
    "formatted": "$2.50",
    "value": 2.5
  },
  "release": {
    "catalog_number": "541125-1, 1-541125 (K1)",
    "resource_url": "https://api.discogs.com/releases/5610049",
    "year": 2014,
    "id": 5610049,
    "description": "LCD Soundsystem - The Long Goodbye: LCD Soundsystem Live At Madison Square Garden (5xLP + Box)",
    "thumbnail": "https://api-img.discogs.com/UsvcarhmrXb0km4QH_dRP8gEf3E=/fit-in/600x600/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-5610049-1399500556-9283.jpeg.jpg"
  },
  "resource_url": "https://api.discogs.com/marketplace/listings/172723812",
  "audio": false
}
```

  * **Response`404`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 404 Not Found  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Content-Type: application/json  
Server: lighttpd  
Content-Length: 33  
Date: Tue, 01 Jul 2014 01:03:20 GMT  
X-Varnish: 1465521729  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "message": "The request resource was not found."
}
```



Edit A Listing
POST
`/marketplace/listings/{listing_id}{?curr_abbr}`
Edit the data associated with a listing.
If the listing’s status is not `For Sale`, `Draft`, or `Expired`, it cannot be modified – only deleted. To re-list a Sold listing, a new listing must be created.
Authentication as the listing owner is required.
  * **Parameters**
  * 

listing_id
     `number` (required) **Example:** 172723812
The ID of the listing you are fetching 

release_id
     `number` (required) **Example:** 1
The ID of the release you are posting 

condition
     `string` (required) **Example:** Mint
The condition of the release you are posting. Must be one of the following:   
`Mint (M)`  
`Near Mint (NM or M-)`  
`Very Good Plus (VG+)`  
`Very Good (VG)`  
`Good Plus (G+)`  
`Good (G)`  
`Fair (F)`  
`Poor (P)` 

sleeve_condition
     `string` (optional) **Example:** Fair
The condition of the sleeve of the item you are posting. Must be one of the following:   
`Mint (M)`  
`Near Mint (NM or M-)`  
`Very Good Plus (VG+)`  
`Very Good (VG)`  
`Good Plus (G+)`  
`Good (G)`  
`Fair (F)`  
`Poor (P)`  
`Generic` `Not Graded` `No Cover` 

price
     `number` (required) **Example:** 10.00
The price of the item (in the seller’s currency). 

comments
     `string` (optional) **Example:** This item is wonderful
Any remarks about the item that will be displayed to buyers. 

allow_offers
     `boolean` (optional) **Example:** true
Whether or not to allow buyers to make offers on the item. Defaults to `false`. 

status
     `string` (required) **Example:** Draft
The status of the listing. Defaults to `For Sale`. Options are `For Sale` (the listing is ready to be shown on the Marketplace) and `Draft` (the listing is not ready for public display). 

external_id
     `string` (optional) **Example:** 10.00
A freeform field that can be used for the seller’s own reference. Information stored here will not be displayed to anyone other than the seller. This field is called “Private Comments” on the Discogs website. 

location
     `string` (optional) **Example:** 10.00
A freeform field that is intended to help identify an item’s physical storage location. Information stored here will not be displayed to anyone other than the seller. This field will be visible on the inventory management page and will be available in inventory exports via the website. 

weight
     `number` (optional) **Example:** 10.00
The weight, in grams, of this listing, for the purpose of calculating shipping. Set this field to `auto` to have the weight automatically estimated for you. 

format_quantity
     `number` (optional) **Example:** 10.00
The number of items this listing counts as, for the purpose of calculating shipping. This field is called “Counts As” on the Discogs website. Set this field to `auto` to have the quantity automatically estimated for you.


  * **Request** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`204`**


Delete A Listing
DELETE
`/marketplace/listings/{listing_id}{?curr_abbr}`
Permanently remove a listing from the Marketplace.  
Authentication as the listing owner is required.
  * **Parameters**
  * 

listing_id
     `number` (required) **Example:** 172723812
The ID of the listing you are fetching


  * **Request** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`204`**