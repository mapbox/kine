var Dynalite = require('dynalite');
var Kinesalite = require('kinesalite');


var dynalite = Dynalite({createTableMs: 0});
var kinesalite = Kinesalite({createStreamMs: 0, shardLimit:100});

module.exports.init = function(t) {
  dynalite.listen(4567, function(err) {
    t.error(err);
    console.log('Dynalite started on port 5567');
    kinesalite.listen(5568, function(err) {
      t.error(err);
      console.log('Kinesalite started on port 5568');
      t.end();
    });
  });
};

module.exports.teardown = function(t) {
  dynalite.close(function(err) {
    t.error(err);
    console.log('Dynalite closed');
    kinesalite.close(function(err) {
      t.error(err);
      console.log('Dynalite closed');
      t.end();
    });
  });
};
