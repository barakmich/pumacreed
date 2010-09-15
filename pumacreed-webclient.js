var voidfn = function() {};

var jobs = {};
var remote_host = null;

function new_job(jobname, code, cb) {

    var job = {
        name: jobname
        data: code.data
    };
    if (code.mapfn !== null)
        eval("job.mapfn = " +code.mapfn);
    if (code.reducefn !== null)
        eval("job.reducefn = " + code.reducefn);
    if (code.initfn != null)
        eval("job.initfn = " + code.initfn);
    if (job.initfn)
        job.initfn(job);
    jobs[jobname] = job;
    document.write("New Job: " + jobname + "\n");
}

function do_map(jobname, kv_list, cb) {
    var out = {jobname: jobname, data: []};
    for (var i in kv_list) {
        var kv = kv_list[i];
        var k = kv[0]
        var v = kv[1]
        jobs[jobname].mapfn(k, v, function(k, v) { var res = [k,v]; out.data.push(res)}, job);
    }
    cb(out);
}

function do_reduce(jobname, kv_list, cb) {
    var out = {jobname: jobname, data: []};
    for (var i in kv_list) {
        var kv = kv_list[i];
        var k = kv[0]
        var v = kv[1]
        jobs[jobname].reducefn(k, v, function(k, v) { var res = [k, v]; out.data.push(res)}, job);
    }
    cb(out);
}

DNode({
    new_job: new_job,
    do_map: do_map,
    do_reduce: do_reduce
}).connect(function (remote) {
    remote_host = remote;
    remote.new_cub("Me!", voidfn);
});
