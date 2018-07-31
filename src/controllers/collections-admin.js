
var controller = module.exports = {
  find: function (req, res) {
    res.send({ok: 1});
  },
  create: function (req, res) {
    console.log('create');
    console.log(req.body);
    var obj = new req.mongoSchemaManager.models.Collection(req.body);
    obj._id = req.params.id;
    console.log(obj.toJSON());
    obj.save(function (err, savedObj) {
      if (err) {
        res.json({ok: 0, error: err.message});
      }
      else res.json({ok: 1, collection: savedObj});
    });
    return;
    req.mongoSchemaManager.models.Collection.create(req.params.id, req.body, function (err, result) {
      if (err) res.json({ok: 0, error: err.message});
      else res.json(result);
    });
  },
  update: function (req, res) {
    req.mongoSchemaManager.models.Collection.update({_id: req.params.id}, req.body, function (err, result) {
      if (err) res.json({ok: 0, error: err.message});
      else res.json(result);
    });
  },
  saveAndProcess: async function (req, res) {
    await req.mongoSchemaManager.models.Collection.update({_id: req.params.id}, req.body);
    var CollectionProcess = require('../collection-process');
    var collectionProcess = new CollectionProcess(req.params.id, req.mongoSchemaManager);
    collectionProcess.run({analyzeSchema: true});
    res.send({ok: 1});
  },
  delete: function (req, res) {
    req.mongoSchemaManager.models.Collection.findById(req.params.id, {dependencies: 1}, function (err, collection) {
      collection.remove(function (err) {
        if (err) res.json({ok: 0, error: err.message});
        else res.send({ok: 1});
      });
    });
  },
  analyze: async function (req, res) {
    var collection = await req.mongoSchemaManager.models.Collection.findById(req.params.id); 
    var options = req.body || {};
    collection.analyze(options);
    res.json({ok: 1});
  },
  overwriteSchema: function (req, res) {
    req.mongoSchemaManager.models.Collection.update({_id: req.params.id}, {'$set': { schemaFields: req.body }}, function (err, result) {
      if (err) res.json(500, {ok: 0, error: err.message});
      else res.json(result);
    });
  },
  mergeSchema: function (req, res) {
  },
  process: function (req, res) {
    var CollectionProcess = require('../collection-process');
    var collectionProcess = new CollectionProcess(req.params.id, req.mongoSchemaManager);
    collectionProcess.run({analyzeSchema: req.query.analyzeSchema || false});
    res.send({ok: 1});
  },
  getDependencies: function (req, res) {
    var id = req.params.id;
    console.log(id);
    req.mongoSchemaManager.models.Collection.find({}, {_id: 1, source: 1, dependencies: 1}, function (err, ucols) {
      if (err) res.status(500).send(err.message);
      else {
        var cols = {};
        ucols.forEach(function (ucol) {
          var col = { dependencies: [], dependents: [], missing: [] };
          if (ucol.source) {
            col.dependencies.push(ucol.source);
          }
          if (ucol.dependencies && ucol.dependencies.length) {
            col.dependencies = ucol.dependencies;
          }
          cols[ucol._id] = col;
        });
        Object.keys(cols).forEach(function (colId) {
          var col = cols[colId];
          col.dependencies.forEach(function (depId) {
            if (typeof cols[depId] === 'undefined') {
              cols[colId].missing.push(colId);
            }
            else cols[depId].dependents.push(colId);
          });
        });
        res.json(cols[id].dependents);
      }
    });
  },
  loadTests: function (req, res) {
    req.mongoSchemaManager.models.CollectionTest.find(function (err, docs) {
      res.json(docs);
    });
  },
  runTests: async function (req, res) {
    var query = req.query ? req.query : {};
    var testDocs = await req.mongoSchemaManager.models.CollectionTest.find(query);
    var num = testDocs.length;
    res.send({ok:1});
    console.log('There are %d tests to run..', num);
    for (var i = 0; i < num; i++) {
      var testDoc = testDocs[i];
      testDoc.runTest(req.msm, true);
    }
  },
  loadTest: async function (req, res) {
    var testDoc = await req.mongoSchemaManager.models.CollectionTest.findById(req.params.id);
    res.json(testDoc);
  },
  saveTest: async function (req, res) {
    try {
      var result = await req.mongoSchemaManager.models.CollectionTest.update({_id: req.params.id}, req.body, {upsert: true});
      res.json(result);
      return;
    }
    catch (err) {
      res.status(404).json({ok: 0, error: err.message});
    }
  },
  runTest: async function (req, res) {
    try {
      var testDoc = await req.mongoSchemaManager.models.CollectionTest.findById(req.params.id);
      var result = await testDoc.runTest();
      res.json({ok: 1, result: result});
    }
    catch (err) {
      res.json({ok: 0, error: err.message});
      return;
    }
  },
  renameCollection: async function (req, res) {
    try {
      var newId = await req.mongoSchemaManager.models.Collection.renameCollection(req.params.id, req.query.newName);
      res.json({ok: 1, id: newId});
    }
    catch (err) {
      res.json({ok: 0, error: err.message});
      return;
    }
  }
}
