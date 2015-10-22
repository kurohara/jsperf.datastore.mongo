/**
 * jsperf.datastore.mongo.js
 * MongoDB data store module for jsperf suite.
 * 
 *
 * Copyright(C) 2015 Hiroyoshi Kurohara(Microgadget,inc.) all rights reserved.
 */
var MongoClient = require('mongodb').MongoClient;

var MongoStore = function MongoStore() {
  this.db = null;
  this.lastInsert = null;
  this.operations = [];
  this.url = "mongodb://localhost:27017/";
  this.dbname = "statdata";
  this.collectionName = "test";
};

MongoStore.prototype = {
  _finished: function (error, result) {
    var lastOp;
    if (error === null && result === null) {
      lastOp = { op: null };
    } else {
      lastOp = this.operations.shift();
    }
    try {
      switch (lastOp.op) {
      case 'CONNECT':
	this.db = result;
	break;
      case 'INSERT':
	break;
      default:
	break;
      }
      if (lastOp.cb) {
	lastOp.cb(error, result);
      }
      
      // for next operataion
      if (this.operations.length > 0) {
	var op = this.operations[0];
	this.op = op.op;
	this.cb = op.cb;
	switch (op.op) {
	case 'CONNECT':
	  MongoClient.connect(this.url + this.dbname, this.options, this._finished.bind(this));
	  break;
	case 'INSERT':
	  if (this.db) {
	    this.lastInsert = null;
	    var collection = this.db.collection(this.collectionName);
	    collection.bulkWrite(op.data, op.options, this._finished.bind(this));
	  }
	  break;
	case 'FETCH':
	  break;
	case 'CLOSE':
	  if (this.db) {
	    this.db.close(true, this._finished.bind(this));
	  }
	  break;
	default:
	  break;
	}
      }
    } catch(e) {
      console.log(e);
    }
  },
  connect: function(url, opts) {
    if (url) {
      this.url = url;
    }
    this.options = (opts ? opts : {});
    this.operations.push({ op: "CONNECT" });
    this._finished(null, null);
  },
  database: function(dbname) {
    this.dbname = dbname;
    if (this.db) {
      this.close();
      this.connect(this.url, this.options);
    }
  },
  collection: function(colname) {
    this.collectionName = colname;
  },
  fetch: function(condition) {
  },
  insert: function(jsondata) {
    var insertOp;
    var qempty = this.operations.length === 0 ? true : false;
    if (this.lastInsert) {
      insertOp = this.lastInsert;
    } else {
      insertOp = { op: 'INSERT', data: [] };
      this.operations.push(insertOp);
      this.lastInsert = insertOp;
    }
    insertOp.data.push({ insertOne: jsondata });
    if (qempty) {
      this._finished(null, null);
    }
  },
  close: function() {
    var qempty = this.operations.length === 0 ? true : false;
    this.operations.push({ op: 'CLOSE' });
    if (qempty) {
      this._finished(null, null);
    }
  },
};
MongoStore.prototype.constructor = MongoStore;

module.exports = MongoStore;
////
