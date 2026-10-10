## Change inventory 
Change inventory
POST
`/inventory/upload/change`
Upload a CSV of listings to change in your inventory.
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