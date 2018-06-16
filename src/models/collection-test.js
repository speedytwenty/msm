var
  assert = require('assert'),
  mongoose = require('mongoose'),
  asyncEval = require('async-eval');

var evalTestUnit = async function (testUnit, testDoc) {
      var testExpr = {
        sumUnemptyString: function (val) { return { '$sum': { '$cond': { if: { '$and': [ { '$eq': [{ '$type': val}, 'string'] }, { '$ne': [val, ''] } ]}, then: 1, else: 0 }}}; },
        sumUnemptyStringOrNull: function (val) { return { '$sum': { '$cond': {
            if: {'$or': [
              { '$and': [ { '$eq': [{ '$type': val}, 'string'] }, { '$ne': [val, ''] } ]},
              {'$eq': [{'$type': val}, 'null']},
              {'$eq': [{'$type': val}, 'missing']},
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumIsString: function (val) { return { '$sum': { '$cond': { if: {'$eq': [{ '$type': val}, 'string'] }, then: 1, else: 0 }}}; },
        sumInvalidString: function (val) { return { '$sum': { '$cond': {
            if: { '$or': [
              { '$ne': [{ '$type': val}, 'string'] },
              { '$eq': [val, ''] } ]},
            then: 1,
            else: 0
          }}};
        },
        sumIsInteger: function (val) { return {'$sum': {'$cond': {if: {'$eq': [{ '$type': val}, 'int']}, then: 1, else: 0 }}}; },
        sumPositiveInteger: function (val) { return {'$sum': {'$cond': {if: {'$and': [{'$eq': [{ '$type': val}, 'int']}, {'$gt': [val, 0]}]}, then: 1, else: 0 }}}; },
        sumInvalidPositiveInteger: function (val) { return {'$sum': {'$cond': {
            if: { '$and': [
              {'$ne': [{ '$type': val}, 'int']},
              {'$lte': [val, 0]}
            ]},
            then: 1,
            else: 0 }}};
        },
        sumIsNumber: function (val) { return {'$sum': {'$cond': {
            if: {'$or': [
              {'$eq': [{ '$type': val}, 'int']},
              {'$eq': [{ '$type': val}, 'double']},
              {'$eq': [{ '$type': val}, 'long']},
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumPositiveNumber: function (val) { return {'$sum': {'$cond': {if: {'$gt': [val, 0]}, then: 1, else: 0 }}}; },
        sumExistsAsBoolean: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [val, { '$ne': [{ '$type': val}, 'bool'] }]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsInteger: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [val, { '$ne': [{ '$type': val}, 'int'] }]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsNumber: function (val) { return { '$sum': { '$cond': {
          if: {'$and': [val, { '$ne': [{ '$type': val}, 'int'] }, { '$ne': [{ '$type': val}, 'double']}, {'$ne':[{'$type': val}, 'long']}]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsPositiveInteger: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [val, { '$ne': [{ '$type': val}, 'int'] }, {'$lte':[val,0]}]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsPositiveIntegerOrNull: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [
              val, { '$ne': [{ '$type': val}, 'int'] }, {'$lte':[val,0]}, {'$ne': [{'$type': val}, 'missing']}
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumPositiveIntegerOrNull: function (val) { return {'$sum': {'$cond': {
            if: {'$or': [
              {'$and': [{'$eq': [{ '$type': val}, 'int']}, {'$gt': [val, 0]}]},
              {'$eq': [{'$type': val}, 'missing']} // check for null type
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumIsIntegerOrNull: function (val) { return {'$sum': {'$cond': {
            if: {'$or': [
              {'$eq': [{ '$type': val}, 'int']},
              {'$eq': [{'$type': val}, 'missing']} // check for null type
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsString: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [val, { '$ne': [{ '$type': val}, 'string'] }]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsUnemptyString: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [val, {'$ne': [val, '']}, { '$ne': [{ '$type': val}, 'string'] }]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsUnemptyStringOrNull: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [
              val, {'$ne': [val, '']}, { '$ne': [{ '$type': val}, 'string'] },
              {'$ne': [{'$type': val}, 'missing']}
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumEqual: function (val1, val2) { return { '$sum': {'$cond': {if: {'$eq': [val1,val2]}, then: 1, else: 0}}} },
        sumBetween: function (val, start, end) { return { '$sum': {'$cond': {if: {'$and': [{'$gte': [val,start]},{'$lte':[val,end]}]}, then: 1, else: 0 }}}; },
        sumGreaterThan: function (val, gtv) { return { '$sum': {'$cond': {if: {'$gt': [val,gtv]}, then: 1, else: 0 }}}; },
        sumGreaterThanEqualTo: function (val, gtev) { return { '$sum': {'$cond': {if: {'$gte': [val,gtev]}, then: 1, else: 0 }}}; },
        sumLessThan: function (val, ltv) { return { '$sum': {'$cond': {if: {'$lt': [val,ltv]}, then: 1, else: 0 }}}; },
        sumLessThanEqualTo: function (val, ltev) { return { '$sum': {'$cond': {if: {'$lte': [val,ltev]}, then: 1, else: 0 }}}; },
        sumIsObject: function (val) { return { '$sum': { '$cond': { if: { '$eq': [{ '$type': val}, 'object'] }, then: 1, else: 0 } } }; },
        sumExistsAsObject: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [val, { '$ne': [{ '$type': val}, 'object'] }]}, 
            //if: {'$and': [{'$ne': [val, undefined]}, { '$ne': [{ '$type': val}, 'object'] }]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsArray: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [val, { '$ne': [{ '$type': val}, 'array'] }]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsArrayWithValues: function (val) { return { '$sum': { '$cond': {
          if: {'$and': [val, { '$ne': [{ '$type': val}, 'array'] }, {'$lte': [{'$size': val}, 0]}]}, 
            then: 1,
            else: 0
          }}};
        },
        sumNotObject: function (val) { return { '$sum': { '$cond': { if: { '$ne': [{ '$type': val}, 'object'] }, then: 1, else: 0 } } }; },
        sumExistsAsDate: function (val) { return { '$sum': { '$cond': {
            if: {'$and': [val, { '$ne': [{ '$type': val}, 'date'] }]}, 
            then: 1,
            else: 0
          }}};
        },
        sumExistsAsValidDate: function (val, md = null) {
          md = md ? md : new Date(1000); // 1970-01-01T00:00:01 - Detect empty dates
          return { '$sum': { '$cond': {
            if: {'$and': [val, { '$ne': [{ '$type': val}, 'date'] }, { '$lte': [val, md] }] },
            then: 1,
            else: 0
          }}};
        },
        sumNotObject: function (val) { return { '$sum': { '$cond': { if: { '$ne': [{ '$type': val}, 'object'] }, then: 1, else: 0 } } }; },
        sumIsArray: function (val) { return { '$sum': { '$cond': { if: { '$eq': [{ '$type': val}, 'array'] }, then: 1, else: 0 } } }; },
        sumIsBoolean: function (val) { return { '$sum': { '$cond': { if: { '$eq': [{ '$type': val}, 'bool'] }, then: 1, else: 0 } } }; },
        sumInvalidOrEmptyArray: function (val) {
          return { '$sum': { '$cond': {
            if: { '$or': [
              {'$ne': [{ '$type': val}, 'array'] },
              {'$lte': [{ '$size': val}, 0] },
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumIsArrayWithValues: function (val) {
          return { '$sum': { '$cond': {
            if: { '$and': [
              {'$eq': [{ '$type': val}, 'array'] },
              {'$gte': [{ '$size': val}, 1] },
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumIsArrayExpectingExactValues: function (val, num = 1) {
          return { '$sum': { '$cond': {
            if: { '$and': [
              {'$eq': [{ '$type': val}, 'array'] },
              {'$eq': [{ '$size': val}, num] },
            ]},
            then: 1,
            else: 0
          }}};
        },
        sumValidDate: function (val, md = null) {
          md = md ? md : new Date(1000); // 1970-01-01T00:00:01 - Detect empty dates
          return { '$sum': { '$cond': {
            if: { '$and': [{ '$eq': [{ '$type': val}, 'date'] }, { '$gte': [val, md] }] },
            then: 1,
            else: 0
          }}};
        }
      };
      var db = testDoc.db.client.db(testDoc.databaseName);
      eval('var evalUnit = async function (testDoc, col, con, db) { ' + testUnit.code + '; };');
  try {
      await evalUnit(testDoc, db.collection(testDoc.collectionName), testDoc.db.client, db);
  }
  catch (err) {
      console.log(err);
      return err.message;
  }
  return null;
}

var CollectionTestSchemaFactory = function (msm) {
  var CollectionTestSchema = mongoose.Schema({
    _id: {
      type: String,
      required: true,
    },
    collectionName: {
      type: String,
      required: true,
    },
    databaseName: {
      type: String,
      required: true,
    },
    error: String,
    unitFailed: Number,
    pass: Boolean,
    units: {
      type: [],
    },
    last: {
      type: Date,
      default: null,
    }
  }, { collection: 'msm_tests' });

  CollectionTestSchema.statics.renameCollection = async function (sourceId, destinationCollectionName, dropSource = true) {
    var document = await this.findOne({_id: sourceId});
    if (document) {
      var destinationId = document.databaseName +':'+destinationCollectionName;
      document._id = destinationId;
      document.isNew = true;
      document.collectionName = destinationCollectionName;
      await document.save();
      if (dropSource) {
        await this.deleteOne({_id: sourceId});
      }
      return destinationId;
    }
  }
  //CollectionTestSchema.plugin(require('mongoose-diff-history/diffHistory').plugin);
  CollectionTestSchema.methods.runTest = async function (msm, lockOnPass = false, verbose = false) {
    this.last = new Date;
    if (verbose) console.log('Running test: %s', this._id);
    var numUnits = this.units.length;
    for (var i = 0; i < numUnits; i++) {
      if (verbose) console.log('  Running unit: %d', i);
      var result = await evalTestUnit(this.units[i], this);
      if (result) {
        this.error = result;
        this.pass = false;
        this.unitFailed = i;
        await this.save();
        return this;
        if (verbose) console.log('  FAIL');
      }
      if (verbose) console.log('  PASS');
    }
    this.pass = true;
    this.error = null;
    this.unitFailed = null;
    await this.save();
    return this;
  }

  return CollectionTestSchema;
}

module.exports = function (mongoose, msm) {
  return mongoose.model(msm.currentServerKey + 'CollectionTest', CollectionTestSchemaFactory(msm));
}
