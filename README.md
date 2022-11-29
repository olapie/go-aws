### Dynamodb
1. Primary Key  
   It is a composite key of partition key and sort key.  
   * Partition Key (Hash Key)
   * Sort Key (Range Key) : optional
2. Main types of actions
   * Single-item requests  
     Act on a single, specific item and require the full primary key.  
     E.g. PutItem, GetItem, UpdateItem, and DeleteItem 
   * Query  
      For table, it reads a range of items with partition key. If sort key is also provided, then it's a single get request. 
      For index, it reads a range of items with partition key and optional sort key. 
   * Scan  
      It reads a range of items but searches across the entire table. This is a slow operation. Try to use query if partition key is determined. 
3. Local secondary index 
   * Use the same partition key which means it can be stored at the same place with origin table item.  
   * It must be created along with the table
4. Global secondary index 
   * Use different partition key as table. If it's the same, why not use local secondary.  
   * If a table has a global secondary index, strong read consistency is not possible, as storage location of index is different with table item's.  
   