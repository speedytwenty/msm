var express = require('express');
var router = module.exports = express.Router();

var collectionsAdmin = require('../../controllers/collections-admin.js');
var databasesAdmin = require('../../controllers/databases-admin.js');
var settingsAdmin = require('../../controllers/settings-admin.js');

router.get('/reset', function (req, res) {
  req.mongoSchemaManager.reset(function (err) {
  });
});

router.put('/settings', settingsAdmin.setMultiple);
router.post('/settings', settingsAdmin.setMultiple);
router.put('/setting/:id', settingsAdmin.setSingle);
router.post('/setting/:id', settingsAdmin.setSingle);
router.delete('/setting/:id', settingsAdmin.unsetSingle);

router.put('/database/:id', databasesAdmin.create);
router.post('/database/:id', databasesAdmin.update);
router.delete('/databsae/:id', databasesAdmin.delete);

router.put('/collection/:id', collectionsAdmin.create);
router.post('/collection/:id/create', collectionsAdmin.create);
router.get('/collection/:id/rename', collectionsAdmin.renameCollection);
router.post('/collection/:id', collectionsAdmin.update);
//router.put('/collection/:id/process', collectionsAdmin.process);
router.get('/collection/:id/process', collectionsAdmin.process);
router.post('/collection/:id/process', collectionsAdmin.saveAndProcess);
router.post('/collection/:id/analyze', collectionsAdmin.analyze);
router.delete('/collection/:id', collectionsAdmin.delete);
//router.put('/collection/:id/schema', collectionsAdmin.overwriteSchema);
router.post('/collection/:id/schema', collectionsAdmin.overwriteSchema);
router.get('/collection/:id/dependents', collectionsAdmin.getDependencies);

router.get('/tests', collectionsAdmin.loadTests);
router.get('/tests/run', collectionsAdmin.runTests);
router.get('/collection/:id/test', collectionsAdmin.loadTest);
router.put('/collection/:id/test', collectionsAdmin.saveTest);
router.post('/collection/:id/test', collectionsAdmin.saveTest);
router.get('/collection/:id/test/run', collectionsAdmin.runTest);
