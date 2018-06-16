'use strict'

var Mongoose = require('mongoose');

var CollectionSchemaFactory = function (mongoose, msm) {
  var Database = require('./database')(Mongoose, msm);
  var CollectionSchema = Mongoose.Schema({
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
    type: {
      type: String,
      required: true,
    },
    //databaseName: Database.Schema,
    enabled: {
      type: Boolean,
      default: false,
    },
    persist: {
      type: Boolean,
      default: false,
    },
    locked: {
      type: Boolean,
      default: false,
    },
    name: {
      type: String,
      required: true,
    },
    source: {
      type: String,
      default: null,
      index: true,
    },
    schemaFields: {
      type: "Mixed",
    },
    meta: {
      type: "Mixed",
    },
    dependencies: { type: Array, index: true },
    eval: {
      type: "Mixed",
    },
    lastProcessed: {
      type: Date,
    },
    timesProcessed: {
      type: Number,
      default: 0,
    },
    millisProcessed: {
      type: Number,
      default: 0,
    },
    description: {
      type: String,
    },
    preProcess: {
      type: String,
    },
    postProcess: {
      type: String,
    },
    preExecute: {
      type: String,
    },
    postExecute: {
      type: String,
    },
  }, { collection: 'msm_collections' });

  CollectionSchema.statics.create = function (id, values, callback) {
    var obj = new CollectionSchema(values);
    obj._id = id;
    obj.save(() => { callback(obj); });
  }
  CollectionSchema.statics.sortByDependencies = sortByDependencies;
  CollectionSchema.statics.renameCollection = async function (sourceId, destinationCollectionName, dropSource = true) {
    if (!destinationCollectionName) {
      throw new Error('Rename Error: Missing destinationCollectionName.');
    }
    var document = await this.findOne({ _id: sourceId});
    if (!document) {
      throw new Error('Rename Error: Source collection ('+sourceId+') does not exist.');
    }
    var sourceCollectionName = document.collectionName;
    var prefix = sourceId.split(':')[0];
    var destinationId = [document.databaseName, destinationCollectionName].join(':');
    var destinationExists = await this.count({_id: destinationId});
    if (destinationExists) {
      throw new Error('Rename Error: Destination collection ('+destinationId+') already exists!');
    }
    document._id = destinationId;
    document.collectionName = destinationCollectionName;
    document.isNew = true;
    await document.save();

    if (dropSource) {
      var colDocs = await this.find({source: sourceId})
      var num = colDocs.length;
      for (var n = 0; n < num; n++) {
        var colDoc = colDocs[n];
        colDoc.source = destinationId;
        if (colDoc.eval && colDoc.eval.code) {
          colDoc.eval.code = stringReplace(colDoc.eval.code, sourceId, destinationId);
          colDoc.eval.code = stringReplace(colDoc.eval.code, sourceCollectionName, destinationCollectionName);
          colDoc.markModified('eval.code');
        }
        await colDoc.save();
      }
      colDocs = await this.find({dependencies: sourceId})
      num = colDocs.length;
      for (var n = 0; n < num; n++) {
        var colDoc = colDocs[n];
        var newDeps = [];
        colDoc.dependencies.forEach(function (dependency, idx) {
          if (dependency == sourceId) {
            newDeps.push(destinationId);
          }
          else newDeps.push(dependency);
        });
        colDoc.dependencies = newDeps;
        if (colDoc.eval && colDoc.eval.code) {
          colDoc.eval.code = stringReplace(colDoc.eval.code, sourceId, destinationId);
          colDoc.eval.code = stringReplace(colDoc.eval.code, sourceCollectionName, destinationCollectionName);
          colDoc.markModified('eval.code');
        }
        await colDoc.save();
      }
      await this.deleteOne({_id: sourceId});
    }
    await msm.models.CollectionTest.renameCollection(sourceId, destinationCollectionName, dropSource);
    await this.db.client.db(document.databaseName).renameCollection(sourceCollectionName, destinationCollectionName, {dropTarget: dropSource});
    return destinationId;
  }

  CollectionSchema.statics.createIdentifier = function (collection) {
    return [collection.database.id, collection.collectionName].join();
  }

  CollectionSchema.statics.list = function (options, callback) {
    this.count(options.query || {}, function (err, count) {
      var query = this.find(options.query || {});
      if (options.project) {
        query.select(options.project);
      }
      if (options.limit) {
        query.limit(1*options.limit);
      }
      if (options.skip) {
        query.skip(options.skip);
      }
      query.exec(function (err, docs) {
        callback(null, {
          total: count,
          collections: sortByDependencies(docs, false),
        });
      });
    }.bind(this));
  }
  CollectionSchema.statics.countDocuments = function (query) {
    return this.count(query);
  }
  CollectionSchema.pre('save', function (next) {
    if (typeof this.id !== 'undefined') {
    }
    if (!this.created) this.created = new Date;
    next();
  });

	CollectionSchema.methods.query = async function (params, callback) {
    params = Object.assign({}, {
        query: {},
        project: false,
        skip: false,
        limit: false,
        batchSize: false,
        sort: false,
    }, params);

    if (!this.schemaFields) {
        return { ok: 0, error: "Collection does not have schema." };
    }

    var col = this.db.client.db(this.databaseName).collection(this.collectionName);
    var result = {
        schema: this.schemaFields,
    };

    if (this.preExecute) {
        eval('var preExecute = async function (document, collection, params, result) { ' + this.preExecute + ' }');
        await preExecute(this, col, params, result);
    }

    var cursor = col.find(params.query);

    result.totalResults = await cursor.count(false);
    if (result.totalResults) {
        var Schema = require('../schema-utils');
        if (params.project) {
            cursor.project(params.project);
            result.schema = Schema.projectSchema(params.project, result.schema);
        }
        if (params.sort) {
            cursor.sort(params.sort);
        }
        if (params.skip) {
            cursor.skip(1*params.skip);
        }
        if (params.limit) {
            cursor.limit(1*params.limit);
        }
        if (params.batchSize) {
            cursor.batchSize(1*params.batchSize);
        }
        result.results = await cursor.toArray();
        result.numResults = result.results.length;

        if (params.flattenResults) {
            result.results = Schema.flattenResults(result.results);
        }
        if (params.flattenSchema) {
            result.schema = Schema.flattenSchemaFields(result.schema);
        }

        if (this.postExecute) {
            eval('var postExecute = async function (document, collection, params, result) { ' + this.postExecute + ' };');
            await postExecute(this, col, params, result);
        }
        result.ok = 1;
        callback(null, result);
    }
	}
	CollectionSchema.methods.analyze = async function (options = {}) {
    var schema = require('../schema-utils');
    options = Object.assign({}, { merge: true }, options);
    if (options.limit) options.limit = 1*options.limit;
		var con = this.db.client;
    if (!this.persist) {
        var db = con.db(this.databaseName);
        var col = db.collection(this.collectionName);
        var tmpColName = 'tmp.' + this.collectionName;
        await col.aggregate([{'$out': tmpColName}]).toArray();
        var result = await schema.analyzeSchema(con, this.databaseName, tmpColName, options);
        db.dropCollection(tmpColName);
    }
    else {
        var result = await schema.analyzeSchema(con, this.databaseName, this.collectionName, options);
    }
    let schemaResult = await schema.extractSchema(con, result.dbName, result.colName);
    con.db(result.dbName).dropCollection(result.colName);

    if (this.schemaFields && options.merge) {
        var sfields = schema.flattenSchemaFields(this.schemaFields);
        for (var f in sfields) {
            Object.assign(sfields[f], {
                percentContaining: 0,
                totalOccurrances: 0,
            });
        }
        schema.expandSchema(sfields,true);
        schemaResult = schema.mergeSchema(schemaResult, schema.expandSchema(sfields));
        //schemaResult = schema.mergeSchema(schemaResult, collection.schema);
    }
		this.schemaFields = schemaResult;
		return this.save();
  }
  CollectionSchema.methods.updateCollectionOptions = async function (options = {}) {
    var optionKeysIn = Object.keys(options);
    var numOptionsIn = optionKeysIn.length;
    if (!numOptionsIn) return;
    var validOptions = ['index', 'noPadding', 'usePowerOf2Sizes', 'validator', 'validationLevel', 'validationAction'];
    var validViewOptions = ['viewOn', 'pipeline'];
    var setOptions = {};
    // Validate the options (at least partially)
    optionKeysIn.forEach(function (optionKey) {
        if (validOptions.indexOf(optionKey) >= 0 || (!this.persist && validViewOptions.indexOf(optionKey) >= 0)) {
            setOptions[optionKey] =  options[optionKey];
        }
        else throw new Error('Invalid collection option: ' + optionKey);
    }.bind(this));
    if (typeof setOptions['pipeline'] !== 'undefined') {
        setOptions['pipeline'] = JSON.stringify(setOptions['pipeline']);
    }
    var db = this.db.client.db(this.databaseName);
    var result = await db.command(Object.assign({ collMod: this.collectionName}, options));
    this.collectionOptions = setOptions;
    await this.save();
  }

  //CollectionSchema.plugin(require('mongoose-diff-history/diffHistory').plugin);
  return CollectionSchema;
}

function sortByDependencies(collections, desc = false) {
  var index = {};
  var visited = {};
  var sortedArr = [];
  function visit(indexItem) {
    var indexId = indexItem.id;
    if (typeof visited[indexId] === 'undefined') {
      visited[indexId] = 1;
      indexItem.dependencies.forEach(function (dependency) {
        if (typeof index[indexId]  === 'undefined') {
          console.log('Collection %s has a missing dependency: %s', indexId, dependency);
          return;
        }
        visit(index[dependency]);
      });
      sortedArr.push(indexItem);
    }
  }
  var index = {};
  var numI = collections.length;
  collections.forEach(function (collection, idx) {
    var indexItem = { id: collection._id, dependencies: [], sourceIdx: idx, enabled: collection.enabled };
    if (collection.source) {
      indexItem.dependencies.push(collection.source);
    }
    if (collection.dependencies) {
      collection.dependencies.forEach(function (dependency) {
        indexItem.dependencies.push(typeof dependency === 'string' ? dependency : dependency._id);
      });
    }
    index[collection._id] = indexItem;
  });

  for (var id in index) {
    visit(index[id]);
  }

  var sortedResult = [];
  sortedArr.forEach(function (indexItem) {
    if (desc) {
      sortedResult.unshift(collections[indexItem.sourceIdx]);
    }
    else sortedResult.push(collections[indexItem.sourceIdx]);
  });
  return sortedResult;
}

function stringReplace(string, subject, replacement) {
  return string.replace(new RegExp(subject, 'g'), replacement);
}

module.exports = function (mongoose, msm) {
  return mongoose.model(msm.currentServerKey + 'Collection', CollectionSchemaFactory(mongoose, msm));
}
