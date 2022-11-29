### Dynamodb
1. Primary Key  
   A composite key which consists of partition key and sort key.  
   * Partition Key (Hash Key) : required 
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
   * <PartitionKey, SortKey> is not unique
   * Determine fields when creating index
4. Global secondary index 
   * Use different partition key as table. If it's the same, why not use local secondary.  
   * If a table has a global secondary index, strong read consistency is not possible, as storage location of index is different with table item's.
   * <PartitionKey, SortKey> is not unique
   * Determine fields when creating index
5. How to implement bidirectional unique constraint? 
   1. Use two tables
      Table1: <k1, k2>
      Table2: <k2, k1> 
      Use transaction to write items into two tables.  
   2. One table with an additional row
      Table: <k1, k2>
      Insert an additional row with joined string "k1+k2" as partition key. 
      This way is not clean
   