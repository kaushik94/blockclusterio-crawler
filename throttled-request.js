var request = require('request');
var cheerio = require('cheerio');
var MongoClient = require('mongodb').MongoClient;

var url = "mongodb://mongo:27017"
var dbo;

MongoClient.connect(url, function(err, db) {
  if (err) throw err;
  console.log("Database created!");
  dbo = db.db("blockcluster");
  dbo.createCollection("crawler", function(err, res) {
    if (err) throw err;
    InitiateThrottledRequest();
    db.close();
  });
});

function updateDB(record, cb) {
  try {
    dbo.collection("crawler").findOneAndUpdate(
       { "url" : record.url },
       { $set: { "url" : record.url, "visits" : record.visits} },
       { upsert:true, returnNewDocument : true },
       (err, docs) => { cb(docs) }
    );
  }
  catch (e){
     console.log(e);
  }
}

const isQualifying = (qualifyingLink) => {
  if (qualifyingLink.indexOf('medium.com') > -1
      && qualifyingLink.indexOf('https://') > -1) {
        return true;
      }
  return false;
}

const getLinks = (body, cb) => {
  $ = cheerio.load(body);
  links = $('a'); //jquery get all hyperlinks
  var finalLinks = [];
  $(links).each(function(i, link){
    // console.log($(link).text() + ':\n  ' + $(link).attr('href'));
    var qualifyingLink = $(link).attr('href');
    if (isQualifying(qualifyingLink)) {
      finalLinks.push(qualifyingLink.split('?')[0]);
    }
  });
  cb(finalLinks)
}

var pendingRequests = ['http://medium.com']
var throttledRequests = {

}
var mapper = {}
var completedRequests = [];

function InitiateThrottledRequest() {
  var eligible = 5 - Object.keys(throttledRequests).length;
  var batch = pendingRequests.splice(0, eligible).map((url) => {
    var requestObject = {
      id: null,
      request: null,
      body: [],
      pending: true,
      completed: false
    }
    var _id = Object.keys(throttledRequests).length;
    requestObject['id'] = _id;
    requestObject['request'] = request
                            .get(url)
                            .on('data', function (chunk) {
                              requestObject['body'].push(chunk);
                            })
                            .on('end', () => {
                              body = Buffer.concat(requestObject['body']).toString();
                              getLinks(body, (links) => {
                                pendingRequests = pendingRequests.concat(links);
                                console.log(mapper)
                                delete throttledRequests[requestObject['id']];
                                if (url in mapper) {
                                  mapper[url] += 1
                                } else {
                                  mapper[url] = 1;
                                }
                                updateDB({url: url, visits: mapper[url]}, (c) => {setTimeout(InitiateThrottledRequest, 5000);});
                              })
                            });
    throttledRequests[_id] = requestObject;
  })
}
