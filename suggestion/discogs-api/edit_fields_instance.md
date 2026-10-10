## Edit Fields Instance 
Edit Fields
POST
`/users/{username}/collection/folders/{folder_id}/releases/{release_id}/instances/{instance_id}/fields/{field_id}{?value}`
Change the value of a notes field on a particular instance.
  * **Parameters**
  * 

username
     `string` (required) **Example:** rodneyfool
The username of the collection you are trying to modify. 

value
     `string` (required) **Example:** foo
The new value of the field. If the field’s type is `dropdown`, the `value` must match one of the values in the field’s list of options. 

folder_id
     `number` (required) **Example:** 3
The ID of the folder to modify. 

release_id
     `number` (required) **Example:** 130076
The ID of the release you are modifying. 

instance_id
     `number` (required) **Example:** 1
The ID of the instance. 

field_id
     `number` (required) **Example:** 8
The ID of the field.


  * **Response`204`**