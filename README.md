# unimodel-elasticsearch

Unimodel library for ElasticSearch.

## Installation

```shell
$ npm install --save unimodel-elasticsearch
```

## Basic Usage

In this section, we will walk through basic usage for the library.

Initiate the default connection to elasticsearch:
```js
let es = require('unimodel-elasticsearch');
es.connect('http://localhost:9200', {
  'warehouse_*': { // index settings for any index matching warehouse_*
    shards: 16,
    replicas: 4
  }
});
```

Create an ElasticsearchModel:
```js
let Animal = es.createModel(
  'Animal', // type name
  { // common-schema specification
    animalId: { type: String, index: true, id: true },
    name: { type: String, index: true }
  },
  'warehouse_animals' // index to store this type in
);
```

Register the ElasticsearchModel with the default model registry:
```js
es.model('Animal', Animal);
```

Use the model registry for CRUD operations on the model:
```js
let Animal = es.model('Animal');
let animal = Animal.create({ animalId: 'dog-charles-barkley', name: 'Charles Barkley' });
animal.save().then(() => {/* after save! */});
```

## Components
For more information on each of these components, see the generated docs.

### ElasticsearchConnection
Connection is a wrapper around an [Elasticsearch Client](https://github.com/elastic/elasticsearch-js),
which ensures a connection is established before allowing operations against the Elasticsearch server.

### ElasticsearchIndexManager
The Index Manager is responsible for storing the index configuration, and instantiating new Indexes when they
are requrested by a Model.  
An ElasticsearchIndexManager should not be directly instantiated, but instead should be created along with a
connection.

### ElasticsearchIndex
An ElasticsearchIndex is responsible for creating, updating, and storing information about an
[ElasticSearch Index](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices.html).  
An ElasticsearchIndex should nto be directly instantiated, but should instead be created by an
ElasticsearchIndexManager.

### ElasticsearchModel
An ElasticserachModel is analogous to a _Type_ in ElasticSearch's langauge. It is responsible for registering
a mapping with the ElasticsearchIndex, and creating/performing bulk operations on ElasticsearchDocuments.

### ElasticsearchDocument
An ElasticsearchDocument directly cooresponds to a _Document_ in ElasticSearch. It is responsible for saving
and removing itself from ElasticSearch.

## Testing

You must have a running elasticsearch instance running on `http://localhost:9200` to run tests.  
Once you have that, simply run:
```shell
$ npm test
```
