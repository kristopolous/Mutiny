## List Orders 
Returns a list of the authenticated user’s orders. Accepts Pagination parameters.
List Orders
GET
`/marketplace/orders{?status,created_after,created_before,sort,sort_order}`
Returns a list of the authenticated user’s orders. Accepts Pagination parameters.
  * **Parameters**
  * 

status
     `string` (optional) **Example:** 1-1
Only show orders with this status. Valid `status` keys are:   
`All`  
`New Order`  
`Buyer Contacted`  
`Invoice Sent`  
`Payment Pending`  
`Payment Received` `In Progress` `Shipped`  
`Merged`  
`Order Changed`  
`Refund Sent`  
`Cancelled`  
`Cancelled (Non-Paying Buyer)`  
`Cancelled (Item Unavailable)`  
`Cancelled (Per Buyer's Request)` `Cancelled (Refund Received)` 

created_after
     `string` (optional) **Example:** 2019-06-24T20:58:58Z
Only show orders created after this ISO 8601 timestamp. 

created_before
     `string` (optional) **Example:** 2019-06-24T20:58:58Z
Only show orders created before this ISO 8601 timestamp. 

archived
     `boolean` (optional) **Example:** true
Only show orders with a specific archived status. If no key is provided, both statuses are returned. Valid `archived` keys are:   
`true`  
`false` 

sort
     `string` (optional) **Example:** 1-1
Sort items by this field (see below). Valid `sort` keys are:   
`id`  
`buyer`  
`created`  
`status`  
`last_activity` 

sort_order
     `string` (optional) **Example:** 1-1
Sort items in a particular order (one of `asc`, `desc`)


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
  "pagination": {
    "per_page": 50,
    "pages": 1,
    "page": 1,
    "items": 1,
    "urls": {}
  },
  "orders": [
    {
      "status": "New Order",
      "fee": {
        "currency": "USD",
        "value": 2.52
      },
      "created": "2011-10-21T09:25:17-07:00",
      "items": [
        {
          "release": {
            "id": 1,
            "description": "Persuader, The - Stockholm (2x12\")",
            "resource_url": "https://api.discogs.com/releases/1",
            "thumbnail": "http://api-img.discogs.com/souG2t4I8ZFVK3kHVtD3zjGvd_Y=/fit-in/300x300/filters:strip_icc():format(jpeg):mode_rgb():quality(40)/discogs-images/R-1-1193812031.jpeg.jpg"
          },
          "price": {
            "currency": "USD",
            "value": 42.0
          },
          "id": 41578242
        }
      ],
      "shipping": {
        "currency": "USD",
        "method": "Standard",
        "value": 0.0
      },
      "shipping_address": "Asdf Exampleton\n234 NE Asdf St.\nAsdf Town, Oregon, 14423\nUnited States\n\nPhone: 555-555-2733\nPaypal address: asdf@example.com",
      "additional_instructions": "please use sturdy packaging.",
      "archived": false,
      "seller": {
        "resource_url": "https://api.discogs.com/users/example_seller",
        "username": "example_seller",
        "id": 1
      },
      "last_activity": "2011-10-21T09:25:17-07:00",
      "buyer": {
        "resource_url": "https://api.discogs.com/users/example_buyer",
        "username": "example_buyer",
        "id": 2
      },
      "total": {
        "currency": "USD",
        "value": 42.0
      },
      "id": "1-1"
      "resource_url": "https://api.discogs.com/marketplace/orders/1-1",
      "messages_url": "https://api.discogs.com/marketplace/orders/1-1/messages",
      "uri": "https://www.discogs.com/sell/order/1-1",
      "next_status": [
        "New Order",
        "Buyer Contacted",
        "Invoice Sent",
        "Payment Pending",
        "Payment Received",
        "In Progress",
        "Shipped",
        "Refund Sent",
        "Cancelled (Non-Paying Buyer)",
        "Cancelled (Item Unavailable)",
        "Cancelled (Per Buyer's Request)"
      ]
    }
  ]
}
```