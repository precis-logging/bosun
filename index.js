var utils = require('precis-utils');
var isTrue = utils.isTrue;
var defaults = utils.defaults;
var Handler = require('./lib/handler').Handler;
var reformFilter = require('./lib/handler').reformFilter;
var path = require('path');
var Joi = require('joi');

var fetchAll = function(from, callback){
  var results = [];
  var fetchBlock = function(offset){
    setImmediate(function(){
      from.asArray({offset: offset}, function(err, records){
        if(err){
          return callback(err);
        }
        if(records[records.root] && records[records.root].length){
          results = results.concat(records[records.root]);
          return fetchBlock(offset+records[records.root].length);
        }
        return callback(null, results);
      });
    });
  };
  fetchBlock(0);
};

var encodeRecord = function(rec, _idOk){
  if(typeof(rec)!=='object' || !rec){
    return rec;
  }
  if(Array.isArray(rec)){
    return rec.map(function(item){
      return encodeRecord(item, true);
    });
  }
  var res = {};
  Object.keys(rec).forEach(function(key){
    if((key === '_id') && (!_idOk)){
      return;
    }
    var newKey = key.replace(/^\$/, '\\$');
    res[newKey] = encodeRecord(rec[key]);
  });
  return res;
};

var listMetrics = function(req, reply){
  var getMetric = function(){
    return this.handler.metrics;
  };
  if(isTrue(utils.defaults({all: true}, req.query).all)){
    return this.store.asArray(req.query, function(err, records){
      if(err){
        return reply(err.toString());
      }
      records[records.root] = records[records.root].map(reformFilter);
      return reply(records);
    });
  }
  var offset = parseInt(req.query.offset)||false;
  var limit = parseInt(req.query.limit)||false;
  var metrics = getMetrics.call(this);
  var res = {
    root: 'metrics',
    metrics: metrics,
    offset: 0,
    limit: metrics.length,
    length: metrics.length,
    count: metrics.length
  };
  if(offset){
    res.offset = offset;
    res.metrics = res.metrics.slice(offset);
  }
  if(limit){
    res.limit = limit;
    res.metrics = res.metrics.slice(0, limit);
  }
  return reply(res);
};

var getMetric = function(req, reply){
  var id = req.params.id;
  var metric = this.handler.metricById(id);
  if(!metric){
    return this.store.get(id, function(err, record){
      if(err){
        return reply(err);
      }
      if(record && record[record.root]){
        return reply(reformFilter(record[record.root]));
      }
      return reply(false);
    });
  }
  return reply(metric);
};

var updateMetric = function(req, reply){
  var id = req.params.id;
  var metric = encodeRecord(req.payload);
  this.store.update(id, metric, function(err, rec){
    if(err){
      return reply(err.toString());
    }
    var metric = rec[rec.root];
    this.handler.updateMetric(id, metric);
    this.sockets.emit('bosun::metric::update', metric);
    return reply(metric);
  }.bind(this));
};

var addMetric = function(req, reply){
  var stat = encodeRecord(req.payload);
  if(!stat.name){
    return reply(new Error('Metric name is required!'));
  }
  if(this.handler.metricExists(stat.name)){
    var err = new Error(`Metric ${stat.name} already exists!`);
    return reply(err.toString());
  }
  this.store.insert(stat, function(err, record){
    if(err){
      return reply(err.toString());
    }
    var metric = record[record.root];
    this.sockets.emit('bosun::metric::update', metric);
    return reply(this.handler.addMetric(metric));
  }.bind(this));
};

var deleteMetric = function(req, reply){
  var id = req.params.id;
  this.store.get(id, function(err, res){
    if(err){
      return reply(err);
    }
    var metric = res[res.root];
    if(!metric){
      return reply(false);
    }
    metric.deleted = true;
    this.store.update(id, encodeRecord(metric), function(err, rec){
      if(err){
        return reply(err.toString());
      }
      this.handler.deleteMetric(id);
      this.sockets.emit('bosun::metric::update', metric);
      return reply(metric);
    }.bind(this));
  }.bind(this));
};

var routes = function(){
  return [
    {
      method: 'GET',
      path: '/api/v1/bosun/metrics',
      config: {
        description: 'Get list of metrics in use',
        tags: ['api'],
        validate: {
          query: {
            all: Joi.boolean().optional(),
            offset: Joi.number().optional(),
            limit: Joi.number().min(1).max(10000).optional(),
            ts: Joi.any().optional(),
          },
        },
        handler: listMetrics.bind(this)
      }
    },
    {
      method: 'GET',
      path: '/api/v1/bosun/metric/{keyOrName}',
      config: {
        description: 'Get the metric settings for {keyOrName}',
        tags: ['api'],
        validate: {
          query: {
            all: Joi.boolean().optional(),
            offset: Joi.number().optional(),
            limit: Joi.number().min(1).max(10000).optional(),
            ts: Joi.any().optional(),
          },
        },
        handler: getMetric.bind(this)
      }
    },
    {
      method: 'POST',
      path: '/api/v1/bosun/metric',
      config: {
        description: 'Create a new metric for {name}',
        tags: ['api'],
        validate: {
          query: {
            all: Joi.boolean().optional(),
            offset: Joi.number().optional(),
            limit: Joi.number().min(1).max(10000).optional(),
            ts: Joi.any().optional(),
          },
        },
        handler: addMetric.bind(this)
      }
    },
    {
      method: 'POST',
      path: '/api/v1/bosun/metric/{id}',
      config: {
        description: 'Update metric for {id}',
        tags: ['api'],
        validate: {
          query: {
            all: Joi.boolean().optional(),
            offset: Joi.number().optional(),
            limit: Joi.number().min(1).max(10000).optional(),
            ts: Joi.any().optional(),
          },
        },
        handler: updateMetric.bind(this)
      }
    },
    {
      method: 'DELETE',
      path: '/api/v1/bosun/metric/{id}',
      config: {
        description: 'Delete the metric {id}',
        tags: ['api'],
        validate: {
          params: {
            id: Joi.string().required(),
          },
        },
        handler: deleteMetric.bind(this)
      }
    },
  ];
};

var registerUi = function(){
  return [
    {
      pages: [
        {
          route: '/bosun/metrics',
          title: 'Metrics',
          name: 'BosunMetrics',
          section: 'Bosun',
          filename: path.resolve(__dirname, 'ui/metrics.jsx'),
        },
      ],
    },
    {
      stores: [
        {
          name: 'BosunMetrics',
          socketEvent: {
            event: 'bosun::metric::update',
            prefetch: '/api/v1/bosun/metrics',
          }
        }
      ]
    },
  ];
};


var Plugin = function(options){
  this.options = options || {};
};

Plugin.prototype.init = function(options){
  var logger = options.logger;
  var sockets = this.sockets = options.sockets;
  var config = this.config = defaults({display: {}}, options);
  var store = this.store = options.stores.get(config.metricsStoreName||'bosun_metrics');
  fetchAll(this.store, function(err, metrics){
    this.metrics = metrics;
    this.handler = new Handler(defaults({
      logger: logger,
      sockets: sockets,
      store: store,
      metrics: metrics,
      url: 'http://localhost:8070/api/put',
    }, config));

  }.bind(this));
};

Plugin.prototype.register = function(options){
  var register = options.register;
  register({
    proxy: options.proxy,
    ui: registerUi.call(this),
    server: routes.call(this)
  });
};

Plugin.prototype.push = function(record){
  if(this.uiOnly){
    return;
  }
  if(!this.handler){
    return setImmediate(function(){
      this.push(record);
    }.bind(this));
  }
  this.handler.push(record);
};

module.exports = Plugin;
