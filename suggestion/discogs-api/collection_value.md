## Collection Value 
Get Collection Value
GET
`/users/{username}/collection/value`
Returns the minimum, median, and maximum value of a user’s collection.
Authentication as the collection owner is required.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to modify.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "maximum": "$250.00",
  "median": "$100.25",
  "minimum": "$75.50"
}
```



Next 
* * *
# User Wantlist