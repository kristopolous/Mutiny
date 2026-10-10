## Delete Instance From Folder 
Delete Instance
DELETE
`/users/{username}/collection/folders/{folder_id}/releases/{release_id}/instances/{instance_id}`
Remove an instance of a release from a user’s collection folder.
To move the release to the “Uncategorized” folder instead, use the `POST` method.
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
The ID of the release you are modifying. 

instance_id
     `number` (required) **Example:** 1
The ID of the instance.


  * **Response`204`**
  * **Response`403`** Toggle
  * ##### Body
```
{
  "message": "You don't have permission to access this resource."
}
```