var DNode = require('dnode');
var sys = require('sys');
var pumaclient = require('pumacreed-client');

var new_job = function(jobdata

var cub_exports = function (client, conn) {
    this.new_job = function () {};
    this.finish_job = function () {};
    this.do_map = pumaclient.do_map;
    this.reduce = function () {};
}
DNode(cub_exports).connect(6060, 
    function (remote) {
    console.log("Connected");
    remote.timesTen(5, function (res) {
        sys.puts(res); // 50, computation executed on the server
    });
});
