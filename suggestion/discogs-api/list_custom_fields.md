## List Custom Fields 
List Fields
GET
`/users/{username}/collection/fields`
Retrieve a list of user-defined collection notes fields. These fields are available on every release in the collection.
If the collection has been made private by its owner, authentication as the collection owner is required.
If you are not authenticated as the collection owner, only fields with public set to true will be visible.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to modify.


  * **Response`200`** Toggle
  * ##### Body
```
{
  "fields": [
    {
      "name": "Media",
      "options": [
        "Mint (M)",
        "Near Mint (NM or M-)",
        "Very Good Plus (VG+)",
        "Very Good (VG)",
        "Good Plus (G+)",
        "Good (G)",
        "Fair (F)",
        "Poor (P)"
      ],
      "id": 1,
      "position": 1,
      "type": "dropdown",
      "public": true
    },
    {
      "name": "Sleeve",
      "options": [
        "Generic",
        "No Cover",
        "Mint (M)",
        "Near Mint (NM or M-)",
        "Very Good Plus (VG+)",
        "Very Good (VG)",
        "Good Plus (G+)",
        "Good (G)",
        "Fair (F)",
        "Poor (P)"
      ],
      "id": 2,
      "position": 2,
      "type": "dropdown",
      "public": true
    },
    {
      "name": "Notes",
      "lines": 3,
      "id": 3,
      "position": 3,
      "type": "textarea",
      "public": true
    }
  ]
}
```