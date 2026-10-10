## Order 
The Order resource allows you to manage a seller’s Marketplace orders.
Get Order
GET
`/marketplace/orders/{order_id}`
View the data associated with an order.  
Authentication as the seller is required.
  * **Parameters**
  * 

order_id
     `number` (required) **Example:** 1-1
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
  "id": "1-1",
  "resource_url": "https://api.discogs.com/marketplace/orders/1-1",
  "messages_url": "https://api.discogs.com/marketplace/orders/1-1/messages",
  "uri": "https://www.discogs.com/sell/order/1-1",
  "status": "New Order",
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
  ],
  "fee": {
    "currency": "USD",
    "value": 2.52
  },
  "created": "2011-10-21T09:25:17-07:00",
  "items": [
    {
      "release": {
        "id": 1,
        "description": "Persuader, The - Stockholm (2x12\")"
      },
      "price": {
        "currency": "USD",
        "value": 42
      },
      "media_condition": "Mint (M)",
      "sleeve_condition": "Mint (M)",
      "id": 41578242
    }
  ],
  "shipping": {
    "currency": "USD",
    "method": "Standard",
    "value": 0
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
    "value": 42
  },
  "tracking": {
    "number": "1Z999999999999999",
    "carrier": "UPS",
    "url": "https://www.ups.com/track?tracknum=1Z999999999999999"
  }
}
```

  * **Response`401`** Toggle
  * ##### Headers
```
Status: HTTP/1.1 401 Unauthorized  
Reproxy-Status: yes  
Access-Control-Allow-Origin: *  
Cache-Control: public, must-revalidate  
Content-Type: application/json  
WWW-Authenticate: OAuth realm="https://api.discogs.com"  
Server: lighttpd  
Content-Length: 61  
Date: Tue, 15 Jul 2014 20:37:49 GMT  
X-Varnish: 1703540564  
Age: 0  
Via: 1.1 varnish  
Connection: keep-alive  

```

##### Body
```
{
  "message": "You must authenticate to access this resource."
}
```



Edit An Order
POST
`/marketplace/orders/{order_id}`
Edit the data associated with an order.  
Authentication as the seller is required.  
The response contains a `next_status` key – an array of valid next statuses for this order, which you can display to the user in (for example) a dropdown control. This also renders your application more resilient to any future changes in the order status logic.  
Changing the order status using this resource will always message the buyer with:
`Seller changed status from Old Status to New Status`
and does not provide a facility for including a custom message along with the change. For more fine-grained control, use the Add a new message resource, which allows you to simultaneously add a message and change the order status.  
If the order status is neither cancelled, Payment Received, nor Shipped, you can change the shipping. Doing so will send an invoice to the buyer and set the order status to Invoice Sent. (For that reason, you cannot set the shipping and the order status in the same request.) Sellers can also set tracking information for an order by providing a `tracking` object with a tracking number and optionally a carrier. The tracking URL will be automatically generated based on the carrier.
  * **Parameters**
  * 

order_id
     `number` (required) **Example:** 1-1
The ID of the order you are fetching 

status
     `string` (optional) **Example:** New Order
The status of the Order you are updating. Must be one of the following:   
`New Order`  
`Buyer Contacted`  
`Invoice Sent`  
`Payment Pending`  
`Payment Received` `In Progress` `Shipped`  
`Refund Sent`  
`Cancelled (Non-Paying Buyer)`  
`Cancelled (Item Unavailable)`  
`Cancelled (Per Buyer's Request)`  
the order’s current status
Furthermore, the new status must be present in the order’s next_status list. For more information about order statuses, see Edit an order. 

shipping
     `number` (optional) **Example:** 5.00
The order shipping amount. As a side-effect of setting this value, the buyer is invoiced and the order status is set to `Invoice Sent`. 

tracking
     `object` (optional)
Set tracking information for the order. Only the seller can set tracking information; buyers will receive a `403 Forbidden` response. The tracking object has the following properties:
    * `number` (required, string): The tracking number provided by the carrier.
    * `carrier` (optional, string): The shipping carrier. Must be one of: `UPS`, `USPS`, `DHL`, `Deutsche Post`, `La Poste`, `Royal Mail`, `PostNL`, `DHL Germany`, `Other`. If not provided, defaults to no carrier specified.


  * **Request** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`200`** Toggle
  * ##### Body
```
{
  "id": "1-1",
  "resource_url": "https://api.discogs.com/marketplace/orders/1-1",
  "messages_url": "https://api.discogs.com/marketplace/orders/1-1/messages",
  "uri": "https://www.discogs.com/sell/order/1-1",
  "status": "Invoice Sent",
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
  ],
  "fee": {
    "currency": "USD",
    "value": 2.52
  },
  "created": "2011-10-21T09:25:17-07:00",
  "items": [
    {
      "release": {
        "id": 1,
        "description": "Persuader, The - Stockholm (2x12\")"
      },
      "price": {
        "currency": "USD",
        "value": 42
      },
      "id": 41578242
    }
  ],
  "shipping": {
    "currency": "USD",
    "method": "Standard",
    "value": 5
  },
  "shipping_address": "Asdf Exampleton\n234 NE Asdf St.\nAsdf Town, Oregon, 14423\nUnited States\n\nPhone: 555-555-2733\nPaypal address: asdf@example.com",
  "additional_instructions": "please use sturdy packaging.",
  "archived": false,
  "seller": {
    "resource_url": "https://api.discogs.com/users/example_seller",
    "username": "example_seller",
    "id": 1
  },
  "last_activity": "2011-10-22T19:18:53-07:00",
  "buyer": {
    "resource_url": "https://api.discogs.com/users/example_buyer",
    "username": "example_buyer",
    "id": 2
  },
  "total": {
    "currency": "USD",
    "value": 47
  },
  "tracking": {
    "number": "1Z999999999999999",
    "carrier": "UPS",
    "url": "https://www.ups.com/track?tracknum=1Z999999999999999"
  }
}
```

  * **Response`400`** Toggle
  * ##### Body
```
{
  "message": "Tracking number is required."
}
```

  * **Response`403`** Toggle
  * ##### Body
```
{
  "message": "Only the seller can set tracking information."
}
```