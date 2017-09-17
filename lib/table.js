module.exports = function(tableName) {
  /***
   * Hold the dynamo table definition
   */
  return {
    'AttributeDefinitions': [
      {
        'AttributeName': 'id',
        'AttributeType': 'S'
      },
      {
        'AttributeName': 'type',
        'AttributeType': 'S'
      }
    ],
    'KeySchema': [
      {
        'AttributeName': 'type',
        'KeyType': 'HASH'
      },
      {
        'AttributeName': 'id',
        'KeyType': 'RANGE'
      }
    ],
    'ProvisionedThroughput': {
      'ReadCapacityUnits': 10,
      'WriteCapacityUnits': 10
    },
    'TableName': tableName
  };
};
