var kue = require('kue');
var queue = kue.createQueue();

queue.process('task', function(job, done){
    console.log('Processing...', job.id, job.data);
    var counter = 0;
    var interval = setInterval(function() {
        job.progress(counter++, 5);
        if (counter === 5) {
            clearTimeout(interval);
            done();
        }
    }, 100);
});
