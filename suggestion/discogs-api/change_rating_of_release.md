## Change Rating Of Release 
Change Rating
POST
`/users/{username}/collection/folders/{folder_id}/releases/{release_id}/instances/{instance_id}`
Change the rating on a release and/or move the instance to another folder.
This endpoint potentially takes 2 `folder_id` parameters: one in the URL (which is the folder you are requesting, and is required), and one in the request body (representing the folder you want to move the instance to, which is optional)
Authentication as the collection owner is required.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to modify. 

folder_id
     `number` (optional) **Example:** 4
The ID of the folder to modify (this parameter is set in the request body and is set if you want to move the instance to this folder). 

release_id
     `number` (required) **Example:** 130076
The ID of the release you are modifying. 

instance_id
     `number` (required) **Example:** 1
The ID of the instance. 

rating
     `number` (optional) **Example:** 5
The rating of the instance you are supplying.


  * **Response`204`**