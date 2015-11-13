var async = require('async');
var L = require('lambda-30').Lambda;
var sift = require('sift');
var extend = require('precis-utils').extend;
var util = require('util');
var url = require('url');
var Wreck = require('wreck');
var makeReform = require('./reform').makeReform;

var noop = function(){};
const DEFAULT_LOGGER = {
  info(){
    console.error.apply(console, arguments);
  },
  error(){
    console.error.apply(console, arguments);
  }
};

var reformFilter = function(source){
  if((typeof(source) !== 'object')||
     (source instanceof Date)||
     (source instanceof RegExp)){
    return source;
  }
  if(source instanceof Array){
    return source.map(reformFilter);
  }
  var res = {};
  Object.keys(source).forEach(function(key){
    var newKey = key.replace(/^\\\$/, '$');
    if(newKey === '_id'){
      return res[newKey] = source[key];
    }
    res[newKey] = reformFilter(source[key]);
  });
  return res;
};

var checkAddItem = function(handler, data, nextItem){
  var logError = function(){
    return handler.options.logger.error.apply(handler.options.logger, arguments);
  }.bind(this);
  var logInfo = function(){
    return handler.options.logger.info.apply(handler.options.logger, arguments);
  }.bind(this);
  var metrics = [];
  var sendItems = function(){
    handler.sendToBosun(metrics);
    nextItem();
  };
  var timestamp = data.time.getTime();

  var aData = data instanceof Array?data:[data];
  var matches = handler.metrics.filter((metric)=>((!metric.disabled) && (sift(metric.filter, aData).length>0)));

  if(matches.length===0){
    return process.nextTick(nextItem);
  }

  return async.each(matches, function(match, next){
    var metric = {
      metric: match.name,
      value: match.value(data),
      timestamp: timestamp,
      tags: match.tags(data),
    };
    metrics.push(metric);
    next();
  }, sendItems);
};

var Handler = function(options){
  this.options = options || {};
  this.uri = this.options.uri || this.options.url || 'http://localhost:8070/';
  this.putUrl = url.resolve(this.uri, '/api/put');
  this.store = options.store;
  this.metrics = [];
  var logger = this.logger = this.options.logger;
  var metrics = this.options.metrics || [];

  logger.info('Loaded metrics: '+metrics.map(function(metric){
    return metric.name;
  }).join(', '));
  metrics.forEach(this.addMetric.bind(this));

  var q = this.q = async.queue(function(data, next){
    checkAddItem(this, data, next);
  }.bind(this), 1);
};

Handler.prototype.sendToBosun = function(pkt){
  var logger = this.logger;
  Wreck.post(
    this.putUrl,
    {
      payload: JSON.stringify(Array.isArray(pkt)?pkt:[pkt]),
      json: true
    },
    function(err, res, payload){
      if(err){
        logger.error(err);
        return;
      }
      logger.debug(this.putUrl);
      logger.debug('Request:', pkt);
      logger.debug('Response:', Buffer.isBuffer(payload)?payload.toString():payload);
      /*
      Wreck.read(res, null, function(err, body){
        if(err){
          logger.error(err);
          return;
        }
        logger.debug(this.putUrl);
        logger.debug(pkt);
        logger.debug(body.toString());
      }.bind(this));
      */
    }.bind(this)
  );
};

Handler.prototype.metricById = function(id){
  var idx = this.metrics.map((metric)=>metric._id).indexOf(id);
  if(idx===-1){
    return false;
  }
  return this.stats[idx];
};

Handler.prototype.metricByName = function(name){
  var idx = this.metrics.map((metric)=>metric.name).indexOf(name);
  if(idx===-1){
    return false;
  }
  return this.metrics[idx];
};

Handler.prototype.metricExists = function(name){
  return !!this.metricByName(name);
};

/*
  Metric: {
    "_id" : ObjectId(),
    "name" : "",
    "disabled" : Boolean,
    "filter" : {
      ...
    },
    "value": Number||Lambda(),
    "tags": {
      ...
    },
    "deleted" : Boolean,
    "_updated" : ISODate(),
    "_type" : "Metrics"
}
*/

var makeMetric = function(record){
  return {
    _id: record._id,
    name: record.name,
    disabled: record.disabled || false,
    deleted: record.deleted || false,
    value: makeReform.call(this, record.value),
    filter: reformFilter.call(this, record.filter||{}),
    tags: makeReform.call(this, record.tags||{}),
  };
};

Handler.prototype.addMetric = function(record){
  if(!record.name){
    throw new Error('Metric name is required');
  }
  if(this.metricByName(record.name)!==false){
    throw new Error('Metric with name of "'+record.name+'" already exists');
  }
  var metric;
  try{
    metric = makeMetric.call(this, record);
  }catch(e){
    metric = record;
    metric.disabled = true;
    logger.error(metric.name);
    logger.error(e.toString());
    if(e.stack){
      logger.error(e.stack);
    }
  }
  this.metrics.push(metric);
  return metric;
};

Handler.prototype.updateMetric = function(id, record){
  if(!record.name){
    throw new Error('Metric name is required');
  }
  var idx = this.metricById(id);
  if(idx===false){
    idx = this.metrics.length;
  }
  var metric = makeMetric.call(this, record);
  this.metrics[idx] = metric;
  return metric;
};

Handler.prototype.deleteMetric = function(id){
  var idx = this.metricById(id);
  if(idx===false){
    return false;
  }
  this.metrics.splice(idx, 1);
  return true;
};

Handler.prototype.push = function(record){
  this.q.push(record);
};

Handler.prototype.processing = function(){
  return this.q.length();
};

Handler.prototype.drain = function(handler){
  this.q.drain = handler;
};

module.exports = {
  Handler,
  reformFilter,
  makeReform,
};
