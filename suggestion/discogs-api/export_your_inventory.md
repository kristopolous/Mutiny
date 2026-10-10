## Export your inventory 
Export your inventory
POST
`/inventory/export`
Request an export of your inventory as a CSV.
  * **Response`200`** Toggle
  * ##### Headers
```
Content-Type: application/json  
Location: https://api.discogs.com/inventory/export/599632  

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