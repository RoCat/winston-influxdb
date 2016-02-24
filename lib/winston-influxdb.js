/**
 * @module winston-influxdb
 * @fileoverview Winston transport for logging into InfluxDB
 * @license MIT
 * @author catoio.romain@gmail.com (Romain CATOIO)
 */
var util = require('util');
var os = require('os');
var influxdb = require('influx');
var winston = require('winston');
var lodash = require('lodash');



/**
 * Constructor for the InfluxDB transport object.
 * @constructor
 * @param {Object} options
 * @param {string=info} options.level Level of messages that this transport
 * should log.
 * @param {string=log} options.db InfluxDB database name
 * @param {Object} options.options InfluxDB connection optional parameters
 * (optional, defaults to `{}`).
 * @param {string=log} options.measurement The name of the measurement you want
 * to store log messages in.
 * @param {string} options.username The username to use when logging into
 * InfluxDB.
 * @param {string} options.password The password to use when logging into
 * InfluxDB. If you don't supply a username and password it will not use InfluxDB
 * authentication.
 * @param {string} options.label Label stored with entry object if defined.
 * @param {string} options.name Transport instance identifier. Useful if you
 * need to create multiple InfluxDB transports.
 * @param {function} options.buildValues A function that build values from level, message, metadata
 * by default to metadata.values. Function also have a callback as last parameter
 * @param {function} options.buildTags A function that build tags from level, message, metadata
 * by default to metadata.tags. Function also have a callback as last parameter
 */
var InfluxDB = exports.InfluxDB = function(options) {
  var self = this;
  winston.Transport.call(this, options);
  options = (options || {});
  self.options = {};
  self.dbOptions = options.options || {};
  self.options.hosts = options.hosts || [
    {
      host: 'localhost',
      port: 8086,
      protocol: 'http'
    }
  ];
  self._queuedLogs = {};
  self.unstackQueuedEventsInterval = options.unstackQueuedEventsInterval || 5000;
  self.maxQueuedEvents = options.maxQueuedEvents || 50;
  self.options.database = options.db || 'log';
  self.options.username = options.username;
  self.options.password = options.password;
  self.dbIsReady = false;
  function onError(err) {
    self.emit('error', err);
  }

  this._queuedLogs = [];
  this.writeLogsInterval = setInterval(function(){
    var queuedLogsToInsert = lodash.clone(self._queuedLogs);
    self._queuedLogs = [];
    self.client.writePoints(self.measurement, queuedLogsToInsert, function(err) {
      if (err) {
        onError(err);
        return;
      }
    });
  }, self.unstackQueuedEventsInterval);

  this.measurement = (options.measurement || 'log');
  this.name = options.name || 'influxdb';
  this.level = (options.level || 'info');

  this.buildValues = options.buildValues || function(level, message, meta, callback){
    callback(null, meta.values);
  };
  this.buildTags = options.buildTags || function(level, message, meta, callback){
    callback(null, meta.tags);
  };

  this._opQueue = [];

  function setupDatabaseAndEmptyQueue(db) {
    authorizeDb(db, function(err) {
      if (err) {
        console.error('winston-influxdb, initialization error: ', err);
        return;
      }
      self.dbIsReady = db.client;
      processOpQueue();
    });
  }

  function processOpQueue() {
    self._opQueue.forEach(function(operation) {
      self[operation.method].apply(self, operation.args);
    });
    delete self._opQueue;
  }

  function authorizeDb(db, cb){
    db.client = influxdb(db.options);
    db.client.getDatabaseNames(function(err,arrayDatabaseNames){
      if(err){
        cb(err);
      } else {
        if(arrayDatabaseNames && arrayDatabaseNames.indexOf(db.database) !== -1){
          cb(null);
        } else {
          db.client.createDatabase(db.database, cb);
        }
      }
    })
  }
  setupDatabaseAndEmptyQueue(this);
};


/**
 * Inherit from `winston.Transport`.
 */
util.inherits(InfluxDB, winston.Transport);

/**
 * Define a getter so that `winston.transports.InfluxDB`
 * is available and thus backwards compatible.
 */
winston.transports.InfluxDB = InfluxDB;

/**
 * Core logging method exposed to Winston. Metadata is optional.
 * @param {string} level Level at which to log the message.
 * @param {string} msg Message to log
 * @param {Object=} opt_meta Additional metadata to attach
 * @param {Function} callback Continuation to respond to when complete.
 */
InfluxDB.prototype.log = function(level, msg, opt_meta, callback) {
  if (!this.dbIsReady) {
    this._opQueue.push({
      method: 'log',
      args: arguments
    });
    return;
  }

  var self = this;

  /**
   * Avoid reentrancy that can be not assumed by database code.
   * If database logs, better not to call database itself in the same call.
   */
  process.nextTick(function() {
    if (self.silent) {
      callback(null, true);
      return;
    }

    function onError(err) {
      self.emit('error', err);
      callback(err, null);
    }

    self.buildValues(level, msg, opt_meta, function(err, values){
      if (err) {
        onError(err);
        return;
      }
      self.buildTags(level, msg, opt_meta, function(err, tags){
        if (err) {
          onError(err);
          return;
        }
        if(!self._queuedLogs){
          self._queuedLogs = [];
        }
        values.time = new Date();
        self._queuedLogs.push([values, tags]);
        if(self._queuedLogs && self._queuedLogs.length >= self.maxQueuedEvents){
          var queuedLogsToInsert = lodash.clone(self._queuedLogs);
          self._queuedLogs = [];
          self.client.writePoints(self.measurement, queuedLogsToInsert, function(err) {
            if (err) {
              console.log(err);process.exit();
              onError(err);
              return;
            }
          });
        }

        self.emit('logged');
        callback(null, true);
      });
    });
  });
};