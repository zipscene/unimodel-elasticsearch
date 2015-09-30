const moment = require('moment');

/**
 * This module contains functions for helping convert between Joda Time conventions.
 */

/**
 * Joda-compatable time format string.
 *
 * @property JODA_ISO_STRING_FORMAT
 * @type String
 * @static
 */
const JODA_ISO_STRING_FORMAT = 'yyyy-MM-ddHH:mm:ss.SSS';
exports.JODA_ISO_STRING_FORMAT = JODA_ISO_STRING_FORMAT;

/**
 * Moment-compatible time format string
 *
 * @property MOMENT_TO_JODA_FORMAT
 * @type String
 * @static
 */
const MOMENT_TO_JODA_FORMAT = 'YYYY-MM-DDHH:mm:ss.sss';
exports.MOMENT_TO_JODA_FORMAT = MOMENT_TO_JODA_FORMAT;

/**
 * Convert a date into Joda-compatable string.
 *
 * @method toJodaFormat
 * @static
 * @param {Date} date - The date to convert.
 * @return {String} The Joda-compatable string.
 */
function toJodaFormat(date) {
	return moment(date).format(MOMENT_TO_JODA_FORMAT);
}
exports.toJodaFormat = toJodaFormat;

/**
 * Convert a Joda-compatable string into an ISO date string.
 *
 * @method fromJodaFormat
 * @static
 * @param {String} jodaStr - The Joda-compatable string.
 * @return {String} Proper ISO date string
 */
function fromJodaFormat(jodaStr) {
	return moment.utc(jodaStr, MOMENT_TO_JODA_FORMAT).toISOString();
}
exports.fromJodaFormat = fromJodaFormat;
