## Add inventory 
Add inventory
POST
`/inventory/upload/add`
Upload a CSV of listings to add to your inventory.
### File structure ¶
The file you upload must be a comma-separated CSV. The first row must be a header with lower case field names.
Here’s an example:
```
release_id,price,media_condition,comments
123,1.50,Mint (M),Comments about this release for sale
456,2.50,Near Mint (NM or M-),More comments
7890,3.50,Good (G),Signed vinyl copy
```

### Things to note: ¶
These listings will be marked “For Sale” immediately. Currency information will be pulled from your marketplace settings. Any fields that aren’t optional or required will be ignored.
### Required fields ¶
  * `release_id` - Must be a number. This value corresponds with the Discogs database Release ID.
  * `price` - Must be a number.
  * `media_condition` - Must be a valid condition (see below).


### Optional fields ¶
  * `sleeve_condition` - Must be a valid condition (see below).
  * `comments` `accept_offer` - Must be Y or N.
  * `location` - Private free-text field to help identify an item’s physical location.
  * `external_id` - Private notes or IDs for your own reference.
  * `weight` - In grams. Must be a non-negative integer.
  * `format_quantity` - Number of items that this item counts as (for shipping).


### Conditions ¶
When you specify a media condition, it must exactly match one of these:
  * `Mint (M)`
  * `Near Mint (NM or M-)`
  * `Very Good Plus (VG+)`
  * `Very Good (VG)`
  * `Good Plus (G+)`
  * `Good (G)`
  * `Fair (F)`
  * `Poor (P)`


Sleeve condition may be any of the above, or:
  * `Not Graded`
  * `Generic`
  * `No Cover`


  * **Parameters**
  * 

upload
     `file` (required)
The CSV file of items to add to your inventory.


  * **Response`200`** Toggle
  * ##### Headers
```
Content-Type: application/json  
Location: https://api.discogs.com/inventory/upload/599632  

```

  * **Response`401`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`409`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`415`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```

  * **Response`422`** Toggle
  * ##### Headers
```
Content-Type: application/json  

```