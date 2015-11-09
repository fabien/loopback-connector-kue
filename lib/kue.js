var _ = require('lodash');
var async = require('async');
var util = require('util');
var assert = require('assert');
var extendModel = require('./model-ext');

var Connector = require('loopback-connector').Connector;
var kue = require('kue');
var Job = kue.Job;

var debug = require('debug')('loopback:connector:kue');
var defaultJobType = 'task';
var defaultLimit = 100;

var STATES = ['complete', 'failed', 'inactive', 'active', 'delayed'];

var PRIORITIES = {
    low: 10,
    normal: 0,
    medium: -5,
    high: -10,
    critical: -15
};

/**
 * Export the Kue class.
 */

module.exports = Kue;

/**
 * Create an instance of the connector with the given `settings`.
 */

function Kue(settings, dataSource) {
    Connector.call(this, 'kue', settings);
    
    assert(typeof settings === 'object', 'cannot initialize Kue without a settings object');
    
    this.debug = settings.debug || debug.enabled;
    this.settings = _.omit(settings, 'defaults');
    this.defaults = _.extend({}, settings.defaults);
    
    _.defaults(this.settings, { limit: defaultLimit });
};

util.inherits(Kue, Connector);

Kue.version = '1.0.0';

Kue.initialize = function(dataSource, callback) {
    var connector = new Kue(_.extend({}, dataSource.settings));
    dataSource.connector = connector; // Attach connector to dataSource
    connector.dataSource = dataSource; // Hold a reference to dataSource
    connector.connect(callback);
};

Kue.prototype.define = function(modelDefinition) {
    Connector.prototype.define.call(this, modelDefinition);
    var self = this;
    var Model = modelDefinition.model;
    extendModel(Model, { scopes: STATES });
};

// Connect and setup proxy event handlers:
//
// - `enqueue` the job is now queued
// - `promotion` the job is promoted from delayed state to queued
// - `progress` the job's progress ranging from 0-100
// - `failed attempt` the job has failed, but has remaining attempts yet
// - `failed` the job has failed and has no remaining attempts
// - `complete` the job has completed

Kue.prototype.connect = function (callback) {
    var ds = this.dataSource;
    this.queue = kue.createQueue(this.settings);
    
    this.queue.on('job enqueue', ds.emit.bind(ds, 'job enqueue'));
    this.queue.on('job promotion', ds.emit.bind(ds, 'job promotion'));
    this.queue.on('job start', ds.emit.bind(ds, 'job start'));
    this.queue.on('job progress', ds.emit.bind(ds, 'job progress'));
    this.queue.on('job failed attempt', ds.emit.bind(ds, 'job failed attempt'));
    this.queue.on('job failed', ds.emit.bind(ds, 'job failed'));
    this.queue.on('job complete', ds.emit.bind(ds, 'job complete'));
    
    this.queue.on('error', ds.emit.bind(ds, 'error'));
    
    process.nextTick(callback);
};

Kue.prototype.disconnect = function() {
    if (this.queue) this.queue.shutdown();
};

Kue.prototype.automigrate = function(models, callback) {
    var self = this;
    if (this.queue && this.queue.client) {
        var client = this.queue.client;
        var key = client.getKey('*');
        client.keys(key, function(err, rows) {
            async.each(rows, function(row, callbackDelete) {
                client.del(row, callbackDelete)
            }, callback);
        });
    } else {
        self.dataSource.once('connected', function() {
            self.automigrate(models, callback);
        });
    }
};

Kue.prototype.getDefaultIdType = function() {
    return Number;
};

Kue.createQueue = function() {
    return kue.createQueue.apply(kue, arguments);
};

Kue.prototype.getDefaults = function(model) {
    var defaults = _.extend({}, this.defaults);
    var modelClass = this._models[model];
    if (modelClass && modelClass.settings.kue) {
        _.extend(defaults, modelClass.settings.kue);
    }
    _.defaults(defaults, { type: defaultJobType });
    return defaults;
};

Kue.prototype.getJobById = function(id, callback) {
    Job.get(id, function(err, job) {
        if (err && err.message && err.message.indexOf('job') === 0) {
            callback(null, job); // ignore missing or invalid
        } else {
            callback(err, job);
        }
    });
};

Kue.prototype.setJobOptions = function(job, options) {
    options = _.extend({}, options);
    
    if (options.priority && _.has(PRIORITIES, options.priority)) {
        job.priority(options.priority);
    }
    
    if (_.isNumber(options.delay) || _.isDate(options.delay)) {
        job.delay(options.delay);
    }
    
    if (_.isNumber(options.attempts)) {
        job.attempts(options.attempts);
    }
    
    if (options.backoff) {
        job.backoff(options.backoff);
    }
    
    if (_.isNumber(options.ttl)) {
        job.ttl(options.ttl);
    }
};

Kue.prototype.jobToData = function(model, job) {
    var idName = this.idName(model);
    var data = _.extend({}, job.data, _.pick(job, idName));
    data.job = job;
    return data;
};

Kue.prototype.getFilterParams = function(model, filter, options) {
    var idName = this.idName(model);
    options = _.extend({}, options);
    var defaults = this.getDefaults(model);
    _.defaults(options, defaults);
    
    var sort = 'asc';
    var offset = 0;
    var limit = _.isNumber(defaults.limit) ? defaults.limit : 0;
    limit = limit || this.settings.limit || defaultLimit;
    
    if (_.isNumber(filter.limit) && filter.limit > 0) limit = filter.limit;
    if (_.isNumber(filter.offset) || _.isNumber(filter.skip)) {
        offset = _.isNumber(filter.offset) ? filter.offset : filter.skip;
    }
    if (_.include(['asc', 'desc'], filter.sort)) sort = filter.sort;
    
    return _.extend({ offset: offset, limit: limit, sort: sort }, options);
};

/**
 * Create a new model instance
 * @param {Function} callback - you must provide the created model's id to the callback as an argument
 */
Kue.prototype.create = function(model, data, options, callback) {
    var defaults = this.getDefaults(model);
    options = _.defaults({}, options, defaults);
    
    var job = this.queue.create(options.type, data);
    this.setJobOptions(job, options);
    
    if (_.isArray(options.searchKeys)) {
        job.searchKeys(options.searchKeys);
    }
    
    job.save(function(err) {
        callback(err, err ? null : job.id);
    });
};
 
/**
 * Check if a model instance exists by id
 */
Kue.prototype.exists = function(model, id, options, callback) {
    this.getJobById(id, function(err, job) {
        callback(err, err ? false : (job ? true : false));
    });
};

/**
 * Find a model instance by id
 * @param {Function} callback - you must provide an array of results to the callback as an argument
 */
Kue.prototype.find = function find(model, id, options, callback) {
    var self = this;
    this.getJobById(id, function(err, job) {
        if (err || !job) return callback(err, null);
        callback(null, self.jobToData(model, job));
    });
};

/**
 * Query model instances by the filter
 */
Kue.prototype.all = function all(model, filter, options, callback) {
    var self = this;
    var idName = this.idName(model);
    filter = _.extend({}, filter);
    
    var params = this.getFilterParams(model, filter, options);
    var client = this.queue.client;
    
    var findById = _.isObject(filter.where) && _.has(filter.where, idName);
    
    if (findById && _.isObject(filter.where[idName])
        && _.isArray(filter.where[idName].inq)) {
        async.map(filter.where[idName].inq, function(id, next) {
            self.getJobById(id, function(err, job) {
                next(err, job ? self.jobToData(model, job) : null);
            });
        }, function(err, jobs) {
            callback(err, _.compact(jobs || []));
        });
    } else if (findById) {
        this.getJobById(filter.where[idName], function(err, job) {
            callback(err, job ? [self.jobToData(model, job)] : []);
        });
    } else if (_.isObject(filter.where) && _.include(STATES, filter.where.state)) {
        getJobsByState(filter.where.state, params, function(err, jobs) {
            if (err || _.isEmpty(jobs)) return callback(err, []);
            var jobToData = self.jobToData.bind(self, model);
            callback(null, _.map(jobs, jobToData));
        });
    } else if (_.isEmpty(filter.where)) {
        // This is a naive implementation, which does not fully
        // respect offset and limit, because it will apply this
        // for each know state instead. However you are rarely
        // interested in a listing of all states mashed together
        // so this is just for completeness, not efficiency.
        
        var match = client.getKey('jobs:' + params.type + ':*');
        client.keys(match, function(err, keys) {
            if (err || _.isEmpty(keys)) return callback(err, []);
            async.map(keys.sort(), function(key, next) {
                var state = _.last(key.split(':'));
                getJobsByState(state, params, next);
            }, function(err, results) {
                if (err) return callback(err, []);
                var jobs = _.flatten(results);
                var jobToData = self.jobToData.bind(self, model);
                callback(null, _.map(jobs, jobToData));
            });
        });
    } else {
        callback(new Error('Invalid filter.'));
    }
};

/**
 * Delete all model instances
 */
Kue.prototype.destroyAll = function destroyAll(model, where, options, callback) {
    this.all(model, { where: where }, options, function(err, jobs) {
        async.each(_.pluck(jobs, 'id'), Job.remove, function(err) {
            callback(err, { count: err ? 0 : jobs.length }); // optimistic count
        });
    });
};

/**
 * Count the model instances by the where criteria
 */
Kue.prototype.count = function count(model, where, options, callback) {
    this.all(model, { where: where }, options, function(err, jobs) {
        callback(err, err ? 0 : jobs.length);
    });
};

/**
 * Update the attributes for a model instance by id
 */
Kue.prototype.updateAttributes = function updateAttributes(model, id, data, options, callback) {
    var self = this;
    this.getJobById(id, function(err, job) {
        if (!err && !job) err = new Error('No ' + model + ' found for id ' + id);
        if (err) return callback(err);
        options = _.extend({}, data.job, options);
        data = _.omit(_.extend({}, data), 'job');
        _.extend(job.data, data);
        self.setJobOptions(job, options);
        job.update(function(err) {
            callback(err, self.jobToData(model, job));
        });
    });
};

/**
 * Save a model instance
 */
Kue.prototype.save = function (model, data, options, callback) {
    var idName = this.idName(model);
    var id = data[id];
    this.updateAttributes(model, id, data, options, callback);
};

/**
 * Update all matching items
 */
Kue.prototype.update =
    Kue.prototype.updateAll = function updateAll(model, where, data, options, callback) {
    var self = this;
    this.all(model, { where: where }, options, function(err, entries) {
        if (err || _.isEmpty(entries)) return callback(err);
        var count = 0;
        options = _.extend({}, data.job, options);
        data = _.omit(_.extend({}, data), 'job');
        async.each(entries, function(entry, next) {
            _.extend(entry.job.data, data);
            self.setJobOptions(entry.job, options);
            entry.job.update(function(err) {
                if (!err) count++;
                next(err);
            });
        }, function(err) {
            callback(err, { count: count });
        });
    });
};

/**
 * Delete a model instance by id
 */
Kue.prototype.destroy = function destroy(model, id, options, callback) {
    var idName = this.idName(model);
    var where = {};
    where[idName] = id;
    this.destroyAll(model, { where: where }, options, callback);
};

// Helpers

function getJobsByState(state, params, callback) {
    Job.rangeByType(params.type, state, params.offset, params.limit, params.sort, callback);
};
