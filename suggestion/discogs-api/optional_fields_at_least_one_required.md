## Optional fields (at least one required) ¶)
  * `price` -
  * `media_condition` - Must be a valid condition (see below).
  * `sleeve_condition` - Must be a valid condition (see below).
  * `comments`
  * `accept_offer` - Must be Y or N.
  * `external_id` - Private notes or IDs for your own reference.
  * `location` - Private free-text field to help identify an item’s physical location.
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
The CSV file of items to alter in your inventory.


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