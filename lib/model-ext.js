var _ = require('lodash');
var async = require('async');
var kue = require('kue');
var Job = kue.Job;

var STATS = [
    'inactiveCount',
    'completeCount',
    'activeCount',
    'failedCount',
    'delayedCount',
    'workTime'
];

module.exports = function(Model, options) {
    
    Model.getJobQueue = function() {
        return this.dataSource.connector.queue;
    };
    
    Model.statistics = function(callback) {
        var connector = this.dataSource.connector;
        var stats = {};
        async.each(STATS, function(call, next) {
            connector.queue[call](function(err, num) {
                stats[call] = _.isNaN(num) ? 0 : num;
                next();
            });
        }, function(err) {
            callback(err, stats);
        });
    };
    
    Model.prototype.getJob = function(force, callback) {
        if (_.isFunction(force)) callback = force, force = false;
        var instance = this;
        if (!force && _.isFunction(callback) && instance.job instanceof Job) {
            process.nextTick(function() {
                callback(null, instance.job);
            });
        } else if (_.isFunction(callback)) {
            Job.get(instance.id, function(err, job) {
                if (err) return callback(err, null);
                instance.job = job;
                callback(null, instance.job);
            })
        } else if (!(instance.job instanceof Job)) {
            instance.job = null;
        }
        return instance.job;
    };
    
    Model.prototype.getLog = function(callback) {
        Job.log(this.id, callback);
    };
    
    // Job integration
    
    Object.defineProperty(Model.prototype, 'job', {
        get: function() {
            return this._job;
        },
        set: function(value) {
            this._job = value;
        },
        configurable: true,
        enumerable: false
    });
    
    Object.defineProperty(Model.prototype, '_job', {
        configurable: true,
        enumerable: false,
        writable: true
    });
    
    var _initProperties = Model.prototype._initProperties;
    Model.prototype._initProperties = function(data, options) {
        if (data.job instanceof Job) this._job = data.job;
        _initProperties.call(this, _.omit(data, 'job'), options);
    };
    
    var toJSON = Model.prototype.toJSON;
    Model.prototype.toJSON = function() {
        var json = toJSON.apply(this, arguments);
        if (this.job) json.job = jobToJSON(this.job);
        return json;
    };
    
    Model.observe('after save', function(ctx, next) {
        var inst = ctx.currentInstance || ctx.instance;
        if (inst && !inst.getJob()) {
            inst.getJob(true, next);
        } else {
            next();
        }
    });
    
    Model.settings.hiddenProperties = Model.settings.hiddenProperties || [];
    Model.settings.hiddenProperties.push('_job');
    
    // Scopes
    
    Model.once('dataAccessConfigured', function() {
        _.each(options.scopes, function(state) {
            Model.scope(state, { where: { state: state } });
        });
    });
    
    // Remoting
    
    Model.statistics.shared = true;
    Model.statistics.http = { verb: 'get', path: '/stats' };
    Model.statistics.returns = { type: 'object', root: true };
    Model.statistics.accessType = 'READ';
    
    Model.prototype.getJob.shared = true;
    Model.prototype.getJob.http = { verb: 'get', path: '/job' };
    Model.prototype.getJob.returns = { type: 'object', root: true };
    Model.prototype.getJob.accessType = 'READ';
    
    Model.prototype.getLog.shared = true;
    Model.prototype.getLog.http = { verb: 'get', path: '/log' };
    Model.prototype.getLog.returns = { type: 'array', root: true };
    Model.prototype.getLog.accessType = 'READ';

};

function jobToJSON(job) {
    var json = _.omit(job.toJSON(), 'data');
    json.started_at = new Date(parseInt(json.started_at, 10));
    json.created_at = new Date(parseInt(json.created_at, 10));
    json.promote_at = new Date(parseInt(json.promote_at, 10));
    json.updated_at = new Date(parseInt(json.updated_at, 10));
    return json;
};