## List Order Messages 
List Order Messages
GET
`/marketplace/orders/{order_id}/messages`
Returns a list of the order’s messages with the most recent first. Accepts Pagination parameters.  
Authentication as the seller is required.
  * **Parameters**
  * 

order_id
     `string` (required) **Example:** 1-1
The ID of the order you are fetching


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
    "items": 8,
    "page": 1,
    "urls": {},
    "pages": 1
  },
  "messages": [
    {
      "refund": {
        "amount": 5,
        "order": {
          "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
          "id": "845236-9"
        }
      },
      "timestamp": "2015-06-02T13:17:54-07:00",
      "message": "example_buyer received refund of $5.00.",
      "type": "refund_received",
      "order": {
        "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
        "id": "845236-9"
      },
      "subject": ""
    },
    {
      "refund": {
        "amount": 5,
        "order": {
          "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
          "id": "845236-9"
        }
      },
      "timestamp": "2015-06-02T13:17:44-07:00",
      "message": "example_seller sent refund of $5.00.",
      "type": "refund_sent",
      "order": {
        "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
        "id": "845236-9"
      },
      "subject": ""
    },
    {
      "from": {
        "id": 1001,
        "username": "example_seller",
        "avatar_url": "https://secure.gravatar.com/avatar/1ddcc19fb43551fb86c143465f773282?s=300&r=pg&d=mm",
        "resource_url": "https://api.discogs.com/users/example_seller"
      },
      "timestamp": "2015-06-02T13:17:07-07:00",
      "message": "Thank you for your order!",
      "type": "message",
      "order": {
        "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
        "id": "845236-9"
      },
      "subject": "New Message - Order #845236-9 - TZ Goes Beyond 10! + 1 more item"
    },
    {
      "status_id": 6,
      "timestamp": "2015-06-02T13:16:57-07:00",
      "actor": {
        "username": "example_seller",
        "resource_url": "https://api.discogs.com/users/example_seller"
      },
      "message": "example_buyer changed the order status to Shipped.",
      "type": "status",
      "order": {
        "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
        "id": "845236-9"
      },
      "subject": ""
    },
    {
      "status_id": 5,
      "timestamp": "2015-06-02T13:16:51-07:00",
      "actor": {
        "username": "example_seller",
        "resource_url": "https://api.discogs.com/users/example_seller"
      },
      "message": "example_buyer changed the order status to Payment Received.",
      "type": "status",
      "order": {
        "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
        "id": "845236-9"
      },
      "subject": ""
    },
    {
      "status_id": 3,
      "timestamp": "2015-06-02T13:16:27-07:00",
      "actor": {
        "username": "example_seller",
        "resource_url": "https://api.discogs.com/users/example_seller"
      },
      "message": "example_buyer changed the order status to Invoice Sent.",
      "type": "status",
      "order": {
        "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
        "id": "845236-9"
      },
      "subject": ""
    },
    {
      "timestamp": "2015-06-02T13:16:27-07:00",
      "original": 0,
      "new": 5,
      "message": "example_seller set the shipping price to $5.00.",
      "type": "shipping",
      "order": {
        "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
        "id": "845236-9"
      },
      "subject": ""
    },
    {
      "status_id": 1,
      "timestamp": "2015-06-02T13:16:12-07:00",
      "actor": {
        "username": "example_seller",
        "resource_url": "https://api.discogs.com/users/example_seller"
      },
      "message": "example_seller created the order by merging orders #845236-7, #845236-8.",
      "type": "status",
      "order": {
        "resource_url": "https://api.discogs.com/marketplace/orders/845236-9",
        "id": "845236-9"
      },
      "subject": ""
    }
  ]
}
```



Add New Message
POST
`/marketplace/orders/{order_id}/messages`
Adds a new message to the order’s message log.  
When posting a new message, you can simultaneously change the order status. If you do, the message will automatically be prepended with:  
`Seller changed status from Old Status to New Status`  
While `message` and `status` are each optional, one or both must be present.
  * **Parameters**
  * 

order_id
     `string` (required) **Example:** 1-1
The ID of the order you are fetching 

message
     `string` (optional) **Example:** hello world 

status
     `string` (optional) **Example:** New Order


  * **Request** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`201`** Toggle
  * ##### Body
```
{
  "from": {
    "username": "example_seller",
    "resource_url": "https://api.discogs.com/users/example_seller"
  },
  "message": "Seller changed status from Payment Received to Shipped\n\nYour order is on its way, tracking number #foobarbaz!",
  "order": {
    "resource_url": "https://api.discogs.com/marketplace/orders/1-1",
    "id": "1-1"
  },
  "timestamp": "2011-11-18T15:32:42-07:00",
  "subject": "Discogs Order #1-1, Stockholm"
}
```

  * **Response`403`** Toggle
  * ##### Body
```
{
  "message": "You don't have permission to access this resource."
}
```