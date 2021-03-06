
var controller = module.exports = {
  find: function (req, res) {
    req.mongoSchemaManager.models.Collection.findOne({ _id: req.params.id }, function (err, collection) {
      if (err) {
        res.status(500).send(err.message);
      }
      if (collection) {
        res.json(collection);
      }
      else res.status(404).send('Page not found.');
    });
  },
  schema: function (req, res) {
    req.mongoSchemaManager.models.Collection.findOne({ _id: req.params.id }, { schemaFields: 1 }, function (err, collection) {
      if (err) {
        res.status(500).send(err.message);
      }
      if (collection) {
        res.json(collection.schemaFields);
      }
      else res.status(404).send('Page not found.');
    });
  },
  count: async function (req, res) {
    try {
      var count = await req.mongoSchemaManager.models.Collection.countDocuments(req.query);
    }
    catch (err) {
      if (err) res.status(500).send(err.message);
      return;
    }
    res.json({ok: 1, count: count});
  },
  analyze: function (req, res) {
  },
  process: function (req, res) {
    var CollectionProcess = require('../collection-process');
    var collectionProcess = new CollectionProcess(req.params.id, req.mongoSchemaManager);
    collectionProcess.run({analyzeSchema: true});
    res.send({ok: 1});
  },
  list: function (req, res) {
    req.mongoSchemaManager.models.Collection.list(req.query, function (err, docs) {
      res.json(docs);
    });
  },
  query: function (req, res) {
    req.mongoSchemaManager.models.Collection.findOne({ _id: req.params.id }, function (err, collection) {
      var params = {
        query: req.query.query || {},
        project: req.query.project || false,
        skip: req.query.skip || false,
        limit: req.query.limit || false,
        batchSize: req.query.batchSize || false,
        sort: req.query.sort || false
      };
      collection.query(params, function (err, results) {
        if (err) res.status(500).send(err.message);
        else res.json(results);
      });
    });
  },
  runTest: function (req, res) {
  },
  loadTest: function (req, res) {
    req.mongoSchemaManager.models.CollectionTest.findOne({ _id: req.params.id }, function (err, collectionTest) {
      if (err) {
        res.status(500).send(err.message);
      }
      if (collectionTest) {
        res.json(collectionTest);
      }
      else res.status(404).send('Page not found.');
    });
  },
}
