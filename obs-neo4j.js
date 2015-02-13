if (Meteor.isClient) {
}

if (Meteor.isServer) {
  Meteor.startup(function () {
    this.N4JDB = new Meteor.Neo4j('http://178.62.59.176:7474/'); 
  });
}
