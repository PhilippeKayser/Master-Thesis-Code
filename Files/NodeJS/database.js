var oracledb = require('oracledb');

var pool;

module.exports.OBJECT = oracledb.OBJECT;

function createPool(config) {
	return new Promise(function(resolve, reject) {
		oracledb.createPool(config, function(err, p){
			if(err)
				return reject(err);

			pool = p;

			resolve(pool);
		});
	});
}

module.exports.createPool = createPool;

function terminatePool() {
	return new Promise(function(resolve, reject) {
		if(pool) {
			pool.terminate(function(err) {
				if(err) {
					return reject(err);
				}

				resolve();
			});
		} else {
			resolve();
		}
	});
}

module.exports.terminatePool = terminatePool;

function getPool() {
	return pool;
}

module.exports.getPool = getPool;

function getConnection() {
	return new Promise(function(resolve, reject){
		pool.getConnection(function(err, connection) {
			if(err) {
				reject(err);
			}

			resolve(connection);
		});
	});
}

module.exports.getConnection = getConnection;

function execute(sql, bindparams, options, connection) {
	options.autoCommit = true;
	return new Promise(function(resolve, reject){
		connection.execute(sql, bindparams, options, function(err, results) {
			if(err) {
				return reject(err);
			}

			resolve(results);
		});
	});
}

module.exports.execute = execute;

function releaseConnection(connection) {
	connection.release(function(err){
		if(err) {
			console.error(err);
		}
	});
}

module.exports.releaseConnection = releaseConnection;

async function executeQuery(sql, bindparams, options) {

	return new Promise(async function(resolve, reject){
		getConnection().then(async function(connection){
			execute(sql, bindparams, options, connection).then(function(results) {
				resolve(results);
				process.nextTick(function(){
					releaseConnection(connection);
				});
			}).catch(function(err){
				reject(err);
				console.log("QUERY NOT EXECUTED:\n"+sql);
				process.nextTick(function() {
					releaseConnection(connection);
				});
			});
		}).catch(function(err){
			reject(err);
		});
	});
}

module.exports.executeQuery = executeQuery;