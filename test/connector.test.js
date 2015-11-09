var DataSource = require('loopback-datasource-juggler').DataSource;
var should = require('should');
var _ = require('lodash');

var ds = new DataSource({
    connector: require('../'),
    defaults: { type: 'job' }
});

var Task = ds.define('Task', {
    name: { type: 'string' }
}, { kue: { type: 'task' } });

var EmailQueue = ds.define('EmailQueue', {
    from: { type: 'string', required: true },
    to: { type: 'string', required: true },
    subject: { type: 'string', required: true },
    body: { type: 'string', required: true },
}, { kue: { type: 'email' } });

var enqueued = [];

ds.on('job enqueue', function(id, type) {
    enqueued.push(id);
});

ds.on('job start', function(id, type) {
    console.log('start:', id, type);
});

ds.on('job progress', function(id, pct) {
    console.log('Progress:', id, pct);
});

ds.on('job complete', function(id) {
    console.log('Completed:', id);
});

describe('Kue Connector', function() {
    
    var job;
    
    var jobData = {
        from: 'fred', to: 'wilma',
        subject: 'Work', body: 'Too much work!'
    };
    
    it('should perform automigration', function(next) {
        ds.automigrate(next);
    });
    
    it('should return the queue instance', function() {
        var queue = EmailQueue.getJobQueue();
        Task.getJobQueue().should.eql(queue);
    });
    
    it('should create a new job', function(next) {
        EmailQueue.create(jobData, { priority: 'critical' }, function(err, inst) {
            if (err) return next(err);
            _.extend(jobData, { id: inst.id });
            inst.__data.should.eql(jobData);
            
            inst.job.should.be.an.object;
            inst.job.save.should.be.a.function;
            
            inst.job.log('Initialized');
            
            job = inst;
            next();
        });
    });
    
    it('should return a job by id - raw find', function(next) {
        ds.connector.find('EmailQueue', 1, {}, function(err, data) {
            if (err) return next(err);
            data.id.should.equal(1);
            data.from.should.equal('fred');
            data.job.should.be.an.object;
            data.job.save.should.be.a.function;
            
            next();
        });
    });
    
    it('should return a job by id', function(next) {
        EmailQueue.findById(1, function(err, inst) {
            if (err) return next(err);
            inst.should.be.instanceof(EmailQueue);
            inst.id.should.equal(1);
            inst.from.should.equal('fred');
            inst.job.should.be.an.object;
            inst.job.type.should.equal('email');
            inst.job.save.should.be.a.function;
            
            var json = inst.toJSON();
            json.id.should.equal(1);
            json.from.should.equal('fred');
            json.job.should.be.an.object;
            json.job.type.should.equal('email');
            json.job.priority.should.equal(-15);
            json.job.created_at.should.be.a.date;
            json.job.promote_at.should.be.a.date;
            json.job.updated_at.should.be.a.date;
            
            inst.getJob().should.equal(inst.job);
            inst.getJob(function(err, job) {
                job.should.equal(inst.job);
                next();
            });
        });
    });
    
    it('should verify that a job exists', function(next) {
        EmailQueue.exists(1, function(err, exists) {
            if (err) return next(err);
            exists.should.be.true;
            next();
        });
    });
    
    it('should return the log for a job', function(next) {
        job.getLog(function(err, log) {
            if (err) return next(err);
            log.should.eql([ 'Initialized' ]);
            next();
        });
    });
    
    it('should return the number of jobs', function(next) {
        EmailQueue.count(function(err, exists) {
            if (err) return next(err);
            exists.should.be.true;
            next();
        });
    });
    
    it('should return the number of jobs - by inactive scope', function(next) {
        EmailQueue.inactive.count(function(err, count) {
            if (err) return next(err);
            count.should.equal(1);
            next();
        });
    });
    
    it('should return the number of jobs - by active scope', function(next) {
        EmailQueue.active.count(function(err, count) {
            if (err) return next(err);
            count.should.equal(0);
            next();
        });
    });
    
    it('should return all jobs', function(next) {
        EmailQueue.find(function(err, instances) {
            if (err) return next(err);
            instances.should.have.length(1);
            instances[0].should.be.instanceof(EmailQueue);
            instances[0].id.should.equal(1);
            next();
        });
    });
    
    it('should return all inactive jobs', function(next) {
        EmailQueue.find({ where: { state: 'inactive' } }, function(err, instances) {
            if (err) return next(err);
            instances.should.have.length(1);
            instances[0].should.be.instanceof(EmailQueue);
            instances[0].id.should.equal(1);
            next();
        });
    });
    
    it('should return all inactive jobs - by scope', function(next) {
        EmailQueue.inactive(function(err, instances) {
            instances.should.have.length(1);
            instances[0].should.be.instanceof(EmailQueue);
            instances[0].id.should.equal(1);
            next();
        });
    });
    
    it('should create another job', function(next) {
        var data = {
            from: 'wilma', to: 'fred',
            subject: 'Busy', body: 'I am busy too!'
        };
        EmailQueue.create(data, function(err, inst) {
            if (err) return next(err);
            inst.id.should.equal(2);
            job = inst;
            
            inst.getJob(function(err, queue) {
                if (err) return next(err);
                queue.save.should.be.a.function;
                var json = queue.toJSON();
                json.priority.should.equal(0);
                next();
            });
        });
    });
    
    it('should return jobs by id', function(next) {
        EmailQueue.findByIds([1, 2], function(err, instances) {
            if (err) return next(err);
            instances.should.have.length(2);
            instances[0].id.should.equal(1);
            instances[1].id.should.equal(2);
            next();
        });
    });
    
    it('should return queue statistics', function(next) {
        EmailQueue.statistics(function(err, info) {
            var expected = {
                inactiveCount: 2,
                completeCount: 0,
                activeCount: 0,
                failedCount: 0,
                delayedCount: 0,
                workTime: 0
            };
            info.should.eql(expected);
            next();
        });
    });
    
    it('should removed a job from the queue', function(next) {
        EmailQueue.destroyById(1, function(err) {
            if (err) return next(err);
            next();
        });
    });
    
    it('should have removed a job from the queue', function(next) {
        EmailQueue.inactive(function(err, instances) {
            instances.should.have.length(1);
            next();
        });
    });
    
    it('should have emitted proxied queue events', function() {
        enqueued.should.eql([1, 2]);
    });
    
    it('should update a queued job', function(next) {
        var data = { subject: 'Busy too?' };
        data.job = { priority: 'high' };
        job.updateAttributes(data, function(err, inst) {
            inst.subject.should.equal('Busy too?');
            inst.from.should.equal('wilma');
            
            var json = inst.job.toJSON();
            json.priority.should.equal(-10);
            next();
        });
    });
    
    it('should have updated a queued job', function(next) {
        EmailQueue.findById(2, function(err, inst) {
            inst.subject.should.equal('Busy too?');
            inst.from.should.equal('wilma');
            
            var json = inst.job.toJSON();
            json.priority.should.equal(-10);
            next();
        });
    });
    
    it('should update all matching jobs', function(next) {
        EmailQueue.updateAll({ state: 'inactive' }, { foo: 'bar' }, function(err, results) {
            results.should.eql({ count: 1 });
            next();
        });
    });
    
    it('should have updated a queued job', function(next) {
        EmailQueue.findById(2, function(err, inst) {
            inst.foo.should.equal('bar');
            next();
        });
    });
    
    it('should create another job type', function(next) {
        Task.create({ name: 'Task 1' }, function(err, inst) {
            inst.job.type.should.equal('task');
            setTimeout(next, 1250);
        });
    });
    
});