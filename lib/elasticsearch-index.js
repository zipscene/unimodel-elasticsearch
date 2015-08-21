
/**
 * This class is instantiated for each Elasticsearch Index (ie, database) in use.
 * It is responsible for initializing the index in ES and maintaining the mappings.
 * Because the mappings have to be registered with the index itself, all models that
 * will be used in this index must be registered with this class.
 *
 * @class ElasticsearchIndex
 * @constructor
 * @param {String} name - The name of this index in ES
 * @param {ElasticsearchConnection} connection - The connection to ES object
 * @param {Object} [config={}] - Configuration settings for the index
 *   @param {Number} [config.shards=5] - Number of shards to split the index into
 *   @param {Number} [config.replicas=1] - Number of replicas for each shard
 *   @param {Object} [config.warmers] - Object (in raw ES format) specifying index warmers.  Passed
 *     directly to ES.
 *   @param {Object} [config.aliases] - Object (in raw ES format) specifying index aliases.  Passed
 *     directly to ES.
 * @param {Object} [options] - Additional options passed to the constructor
 *   @param {Boolean} [options.initialize=true] - If set to false, the index is not automatically
 *     created or synchronized on class construction.
 */
class ElasticsearchIndex extends EventEmitter {

	constructor(name, connection, config, options = {}) {
		// Save these values on the class so they're accessible later, and so if we don't
		// initialize right away, they can be used later.
		this.name = name;
		this.connection = connection;
		this.config = config;

		// Construct a promise on this class that resolves when the index is fully initialized
		// and available.  Because this promise can be resolved in a few different ways, the
		// resolve and reject methods are also stored as (internal) properties on the class.

	}

}
