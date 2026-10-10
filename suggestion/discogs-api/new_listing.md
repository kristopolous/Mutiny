## New Listing 
Create New Listing
POST
`/marketplace/listings{?release_id,condition,sleeve_condition,price,comments,allow_offers,status,external_id,location,weight,format_quantity}`
Create a Marketplace listing.  
Authentication is required; the listing will be added to the authenticated user’s Inventory.
  * **Parameters**
  * 

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

  * **Response`201`** Toggle
  * ##### Body
```
{
  "listing_id": 41578241,
  "resource_url": "https://api.discogs.com/marketplace/listings/41578241"
}
```

  * **Response`403`** Toggle
  * ##### Body
```
{
  "message": "You don't have permission to access this resource."
}
```