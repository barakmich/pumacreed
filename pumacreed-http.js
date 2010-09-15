var http = require('http');
var io = require('./socket.io-node');
var fs = require("fs");
var path = require("path");
var sourcejs = require('dnode/web').source()

var writeFile = function(req, res, filename) {
  try {
    res.writeHead(200, {'Content-Type': 'text/html', 'Connection': 'close'});
    fs.createReadStream( path.normalize(path.join(__dirname, filename)), {
        'flags': 'r',
        'encoding': 'binary',
        'mode': 0666,
        'bufferSize': 4 * 1024
      }).addListener("data", function(chunk){
        res.write(chunk, 'binary');
      }).addListener("end",function() {
        res.end();
      });
  }
  catch (e) {
    res.writeHead(404, {'Content-Type' : 'text/html'});
    res.write("<h1>File not found</h1>");
    res.end();
  }
}

exports.makeHttpServer = function () { return http.createServer(function(req, res){
  if(req.method == "GET"){
    console.log(req.url);
    if( req.url.indexOf("favicon") > -1 ){
      res.writeHead(200, {'Content-Type': 'image/x-icon', 'Connection': 'close'});
      res.end("");
    } else if (req.url.indexOf("pumacreed-webclient.js") > -1) {
        writeFile(req,res,"pumacreed-webclient.js");
    } else if (req.url == "/dnode.js") {
      res.writeHead(200, {'Content-Type': 'text/javascript'});
      res.end(sourcejs);
    } else {
        writeFile(req,res,"index.html");
    } 
  } else {
    res.writeHead(404);
    res.end();
  }
});
};
