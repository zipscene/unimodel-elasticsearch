// Copyright 2016 Zipscene, LLC
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0



/**
 * Replicates the ElasticSearch globbing to filter a list of strings by a glob expression.
 *
 * @method elasticsearchGlobFilter
 * @static
 * @param {String} glob - The glob expression.  Ie, `foo*` or `bar,baz` .
 * @param {String[]} strings - The strings to filter.
 * @return {String[]} - The strings that match the glob.
 */
function elasticsearchGlobFilter(glob, strings) {
	// Currently this only handles globs in the form `foo*` and `bar,baz` .  I'm not sure
	// what else ES supports and can't find a reference.

	// Convert the glob into a regex
	let regex = new RegExp(('(' + glob.replace(/,/g, ')|(') + ')').replace(/\*/g, '.*'));

	// Filter by matches to the regex
	let ret = [];
	for (let str of strings) {
		if (regex.test(str)) {
			ret.push(str);
		}
	}

	return ret;
}

exports.elasticsearchGlobFilter = elasticsearchGlobFilter;
