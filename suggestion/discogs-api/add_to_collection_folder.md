## Add To Collection Folder 
Add Release
POST
`/users/{username}/collection/folders/{folder_id}/releases/{release_id}`
Add a release to a folder in a user’s collection.
The `folder_id` must be non-zero – you can use `1` for “Uncategorized”.
Authentication as the collection owner is required.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to modify. 

folder_id
     `number` (required) **Example:** 3
The ID of the folder to modify. 

release_id
     `number` (required) **Example:** 130076
The ID of the release you are adding.


  * **Response`201`** Toggle
  * ##### Body
```
{
  "instance_id": 3,
  "resource_url": "https://api.discogs.com/users/example/collection/folders/1/release/1/instance/3"
}
```