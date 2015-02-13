var Future = Meteor.npmRequire('fibers/future');

Meteor.methods({
	
	'neo4j/clear': function() {

		return clearDatabase();

	},

	'neo4j/addNode': function(details) {

		this.unblock();
		if (getNode(details._id)) return null;
		var node = addNode(details);
		return node.id;

	},

	'neo4j/getNode': function(_id) {

		this.unblock();
		var node = getNode(_id);
		return node && _.pick(node._data, ['metadata', 'data']);

	},

	'neo4j/addNodes': function(query, options) {

		var futs = {},
			nodes = [],
			ids = {};
		
		this.unblock();

		CommunityNodes.find(query, options).forEach(function(node) {
			if (!getNode(node._id)) {
				var cleanedNode = cleanNode(node);
				nodes.push(N4JDB.createNode(cleanedNode));
				futs[cleanedNode._id] = new Future();
			}
		})

		_.each(nodes, function(node) {
			node.index('node', '_id', node._data.data, function(err, node) {
				node.save(function (err, nodeAdded) {
				    if (err) {
				        console.error('Error saving new node to database:', err);
				        ids[node._data.data] = 'FAIL';
				        futs[node._data.data].throw(err);
				    } else {
				        console.log('Node saved to database with id:', nodeAdded.id);
				        ids[nodeAdded._data.data._id] = node.id;
				        futs[nodeAdded._data.data._id].resolver().call(null);
				    }
				});
			});
		});

		Future.wait.apply(this, _.values(futs));

		return ids;

	},	

	'neo4j/addEdge': function(edge, force) {
		var fut = new Future(),
			source_neo4j = getNode(edge.source_node_id),
			target_neo4j = getNode(edge.target_node_id),
			source_mongo = CommunityNodes.findOne(edge.source_node_id),
			target_mongo = CommunityNodes.findOne(edge.target_node_id),
			rel;

		this.unblock();

		if (getEdge(edge._id)) return null;

		if (!source_neo4j) {
			if (!force || !source_mongo) return false;
			source_neo4j = addNode(cleanNode(source_mongo));
		}
		if (!target_neo4j) {
			if (!force || !target_mongo) return false;
			target_neo4j = addNode(cleanNode(target_mongo));
		}

		rel = createRelationship(source_neo4j, target_neo4j, edge.update_type, _.pick(edge, [
			'project_id',
			'_id',
			'update_type',
			'update_id'
		]));

		return rel.id;

	},

	'neo4j/getEdge': function(_id) {

		this.unblock();
		var edge = getEdge(_id);
		return edge && _.pick(edge._data, ['metadata', 'data']);

	},

	'neo4j/addEdges': function(query, options, force) {

		var futs = {},
			edges = [],
			ids = {};

		Edges.find(query, options).forEach(function(edge) {

			if (getEdge(edge._id)) return null;

			var source_neo4j = getNode(edge.source_node_id),
				target_neo4j = getNode(edge.target_node_id),
				source_mongo = CommunityNodes.findOne(edge.source_node_id),
				target_mongo = CommunityNodes.findOne(edge.target_node_id),
				rel;

			if (!source_neo4j) {
				if (!force || !source_mongo) return false;
				source_neo4j = addNode(cleanNode(source_mongo));
			}
			if (!target_neo4j) {
				if (!force || !target_mongo) return false;
				target_neo4j = addNode(cleanNode(target_mongo));
			}

			futs[edge._id] = new Future();

			createRelationship(source_neo4j, target_neo4j, edge.update_type, _.pick(edge, [
				'project_id',
				'_id',
				'update_type',
				'update_id'
			]), function(err, rel) {

			    if (err) {
			        console.error('Error adding relationship to database:', err);
			        ids[edge._id] = 'FAIL';
			        futs[edge._id].throw(err);
			    } else {
			        console.log('Relationship saved to database with id:', rel.id);
					rel.index('edge', '_id', edge._id, function(err, thisRel) {
				        ids[rel._data.data._id] = rel.id;
				        futs[rel._data.data._id].resolver().call(null);
					});
			    }				

			});			

		});

		Future.wait.apply(this, _.values(futs));

		return ids;
		
	}

});

function cleanNode(node) {

	return _.pick(node, [

		'_id',
		'lat',
		'lng',
		'location_name',
		'project_id',
		'type',
		'value'

	]);

}

function addNode(details) {

	var fut = new Future(),
		node = N4JDB.createNode(details);

	node.save(function (err, node) {
	    if (err) {
	        console.error('Error saving new node to database:', err);
	        fut.throw(err);
	    } else {
	        console.log('Node saved to database with id:', node.id);
	        node.index('node', '_id', details._id, function(err, thisNode) {
	        	fut.return(node);
	        });
	    }
	});

	return fut.wait();

}

function getNode(_id) {

	var fut = new Future();

	N4JDB.query("MATCH (n {_id: {_id}}) \n RETURN n", {_id: _id}, function(err, res) {

		if (err) fut.throw(err);
		else fut.return(res.length && res[0].n);

	});

	return fut.wait();

}

function getEdge(_id) {

	var fut = new Future();

	N4JDB.query("MATCH (n)-[r]-(m) WHERE r._id = {_id} RETURN r", {_id: _id}, function(err, res) {

		if (err) fut.throw(err);
		else fut.return(res.length && res[0].r);

	});

	return fut.wait();

}

function createRelationship(source, target, type, data, cb) {

	var fut = new Future();

	source.createRelationshipTo(target, type, data, cb ? cb : function(err, rel) {

		if (err) fut.throw(err);
		else rel.index('edge', '_id', data._id, function(rel, err) {
			fut.return(rel);
		});

	});

	return cb ? null : fut.wait();

}

function clearDatabase() {

	N4JDB.query("START n = node(*) OPTIONAL MATCH n-[r]-() WHERE (ID(n)>0 AND ID(n)<10000) DELETE n, r;", null, function(err, res) {

		if (err) fut.throw(err);
		else fut.return(true);

	});

}